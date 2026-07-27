use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use distributed_state_ab::observer_protocol::{
    build_sanitized_observer_plan, validate_sanitized_plan_bytes, write_driver_fake_input,
    write_supervisor_bootstrap, DiagnosticReadSecret, ObserverProtocolResultV3,
    ProtocolDispositionV2, SUPERVISOR_CANCEL_BYTES,
};
use distributed_state_ab::{
    seal_base_plan, sha256_bytes, validate_manifest_path, ArtifactIdentityV1, ArtifactSetV1,
    AuthenticationV1, BasePlanV1, DiagnosticRouteV1, DryRunRecordV1,
    FakeProtocolObserverDispositionV1, FakeProtocolRunRecordV1, LifecycleBoundaryV1, LimitsV1,
    ManifestV1, ObserverDispositionV1, ObserverMode, ObserverPolicyV1, ObserverResultV1, PairV1,
    WorkloadV1, MANIFEST_SCHEMA, NOTICE,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use uuid::Uuid;
use zeroize::Zeroize as _;

const SECRET_SENTINEL: &str = "cycle62-secret-7f52d1d9b9874a73a869";
const DIAGNOSTIC_TEST_SECRET: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk";
const WRONG_DIAGNOSTIC_TEST_SECRET: &str = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg";
const DIAGNOSTIC_AUTH_HEADER: &[u8] =
    b"Authorization: Bearer CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk\r\n";
const LOCAL_EVIDENCE_PATH: &str = "/api/v1/cluster/local-evidence";
const TIMING_PATH: &str = "/api/v1/cluster/local-checkpoint-barrier-timings";
const CHILD_COMPLETION_BOUND: Duration = Duration::from_secs(30);
const BOOTSTRAP_FAILURE_BOUND: Duration = Duration::from_secs(8);
const CANCELLATION_BOUND: Duration = Duration::from_secs(8);

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    driver: PathBuf,
    manifest: ManifestV1,
    manifest_path: PathBuf,
}

struct Run {
    output: Output,
    record: DryRunRecordV1,
    plan: Vec<u8>,
    trace: Vec<u8>,
    observer_stdout: Vec<u8>,
    observer_stderr: Vec<u8>,
    artifact_bytes: Vec<u8>,
}

struct FakeDriverRun {
    output: Output,
    record: FakeProtocolRunRecordV1,
    plan: Vec<u8>,
    trace: Vec<u8>,
    artifact_bytes: Vec<u8>,
    artifact_names: Vec<String>,
}

struct BootstrappedFakeProtocol {
    child: Child,
    supervisor: ChildStdin,
    plan_bytes: Vec<u8>,
    sanitized_plan_sha256: String,
    invocation_id: Uuid,
}

#[derive(Clone, Copy)]
enum FakeServerBehavior {
    Success { node_id: u64 },
    BusyThenSuccess { node_id: u64, busy_requests: usize },
    Stall,
}

#[derive(Clone, Copy)]
enum DriverFakeInput {
    Valid(&'static str),
    Invalid,
    HeldOpen,
    PartialHeldOpen,
}

struct OwnedFakeServer {
    address: SocketAddr,
    connections: Arc<AtomicUsize>,
    requests: mpsc::Receiver<Vec<u8>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl OwnedFakeServer {
    fn success(node_id: u64) -> Self {
        Self::spawn(FakeServerBehavior::Success { node_id })
    }

    fn stall() -> Self {
        Self::spawn(FakeServerBehavior::Stall)
    }

    fn busy_then_success(node_id: u64, busy_requests: usize) -> Self {
        Self::spawn(FakeServerBehavior::BusyThenSuccess {
            node_id,
            busy_requests,
        })
    }

    fn spawn(behavior: FakeServerBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (requests_tx, requests) = mpsc::channel();
        let thread_connections = connections.clone();
        let thread_stop = stop.clone();
        let thread = std::thread::spawn(move || loop {
            let (mut stream, _) = listener.accept().unwrap();
            if thread_stop.load(Ordering::Acquire) {
                break;
            }
            stream.set_nodelay(true).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let request_ordinal = thread_connections.fetch_add(1, Ordering::AcqRel);
            let request = read_http_request(&mut stream);
            requests_tx.send(request.clone()).unwrap();
            match behavior {
                FakeServerBehavior::Success { node_id } => {
                    write_success_response(&mut stream, node_id, &request);
                }
                FakeServerBehavior::BusyThenSuccess {
                    node_id,
                    busy_requests,
                } => {
                    if request_ordinal < busy_requests {
                        write_json_response(&mut stream, 503, b"{}");
                    } else {
                        write_success_response(&mut stream, node_id, &request);
                    }
                }
                FakeServerBehavior::Stall => {
                    while !thread_stop.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    break;
                }
            }
        });
        Self {
            address,
            connections,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::Acquire)
    }

    fn drain_requests(&self) -> Vec<Vec<u8>> {
        self.requests.try_iter().collect()
    }
}

impl Drop for OwnedFakeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
        for mut request in self.requests.try_iter() {
            request.zeroize();
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 512];
    while request.len() <= 8 * 1_024 {
        let read = stream.read(&mut chunk).unwrap();
        assert_ne!(read, 0, "fake-server request ended before its headers");
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return request;
        }
    }
    panic!("fake-server request exceeded its test-only header bound");
}

fn write_success_response(stream: &mut TcpStream, node_id: u64, request: &[u8]) {
    if !request
        .windows(DIAGNOSTIC_AUTH_HEADER.len())
        .any(|window| window == DIAGNOSTIC_AUTH_HEADER)
    {
        write_json_response(stream, 403, b"{}");
        return;
    }
    let body = if request.starts_with(format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n").as_bytes())
    {
        local_evidence_body(node_id)
    } else {
        assert!(request.starts_with(format!("GET {TIMING_PATH}?").as_bytes()));
        empty_timing_body(node_id)
    };
    write_json_response(stream, 200, &body);
}

fn write_json_response(stream: &mut TcpStream, status: u16, body: &[u8]) {
    let reason = match status {
        200 => "OK",
        403 => "Forbidden",
        _ => "Service Unavailable",
    };
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    stream.write_all(&response).unwrap();
}

fn boot_incarnation(node_id: u64) -> String {
    format!("00000000-0000-4000-8000-{node_id:06x}000001")
}

fn local_evidence_body(node_id: u64) -> Vec<u8> {
    let boot_incarnation = boot_incarnation(node_id);
    serde_json::to_vec(&json!({
        "schema_version": "laminardb-local-authority-evidence/v1",
        "evidence": {
            "participant": {
                "node_id": node_id,
                "boot_incarnation": boot_incarnation,
            },
            "process_term": 1,
            "adopted_assignment": {
                "participant": {
                    "node_id": node_id,
                    "boot_incarnation": boot_incarnation,
                },
                "assignment_version": 1,
                "partitioning_abi_version": 1,
                "vnode_count": 256,
                "assignment_digest": vec![1_u8; 32],
            }
        }
    }))
    .unwrap()
}

fn empty_timing_body(node_id: u64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "schema_version": "laminardb-local-checkpoint-barrier-timings/v1",
        "process_identity": {
            "participant": {
                "node_id": node_id,
                "boot_incarnation": boot_incarnation(node_id),
            },
            "process_term": 1,
        },
        "after_sequence": 0,
        "page": {
            "capacity": 1_024,
            "oldest_retained_sequence": Value::Null,
            "next_sequence": 1,
            "overwritten_record_count": 0,
            "recording_loss_count": 0,
            "metadata_exhausted": false,
            "has_more": false,
            "records": [],
        }
    }))
    .unwrap()
}

fn wait_for_child(mut child: Child, bound: Duration) -> Output {
    let deadline = Instant::now() + bound;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("observer child exceeded its test wall-clock bound");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut stdout)
        .unwrap();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    Output {
        status,
        stdout,
        stderr,
    }
}

fn wait_for_connections(server: &OwnedFakeServer, expected: usize, bound: Duration) {
    let deadline = Instant::now() + bound;
    while server.connection_count() != expected && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(server.connection_count(), expected);
}

fn passive_loopback_endpoints() -> ([TcpListener; 3], [SocketAddr; 3]) {
    let listeners = std::array::from_fn(|_| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = std::array::from_fn(|index| listeners[index].local_addr().unwrap());
    (listeners, addresses)
}

fn assert_fake_outcome_is_post_end(record: &FakeProtocolRunRecordV1) {
    let end = record
        .supervisor_events
        .iter()
        .position(|event| event == "end_seal_consumed")
        .unwrap();
    for (index, event) in record.supervisor_events.iter().enumerate() {
        if event.contains("post_end") {
            assert!(index > end, "{event:?} occurred before end seal");
        }
    }
}

fn assert_observer_output_redacted(output: &Output) {
    for secret in [
        SECRET_SENTINEL.as_bytes(),
        DIAGNOSTIC_TEST_SECRET.as_bytes(),
    ] {
        assert!(!output
            .stdout
            .windows(secret.len())
            .any(|window| window == secret));
        assert!(!output
            .stderr
            .windows(secret.len())
            .any(|window| window == secret));
    }
}

fn fake_protocol_command(fixture: &Fixture, arm: &str) -> Command {
    let mut command = Command::new(&fixture.manifest.artifacts.observer.path);
    command
        .arg("fake-protocol")
        .arg(&fixture.manifest_path)
        .arg(arm)
        .arg("success")
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    command.env(
        "SystemRoot",
        std::env::var_os("SystemRoot").expect("Windows provides an absolute SystemRoot"),
    );
    command
}

fn spawn_fake_protocol_child(fixture: &Fixture, arm: &str) -> Child {
    fake_protocol_command(fixture, arm).spawn().unwrap()
}

fn spawn_bootstrapped_fake_protocol(
    fixture: &Fixture,
    arm: &str,
    addresses: [SocketAddr; 3],
) -> BootstrappedFakeProtocol {
    let manifest = validate_manifest_path(&fixture.manifest_path).unwrap();
    let base_plan = seal_base_plan(&manifest).unwrap();
    let plan = build_sanitized_observer_plan(&base_plan, addresses).unwrap();
    let plan_bytes = serde_json::to_vec(&plan).unwrap();
    let sanitized_plan_sha256 = validate_sanitized_plan_bytes(&plan_bytes)
        .unwrap()
        .canonical_sha256()
        .to_owned();
    let secret =
        DiagnosticReadSecret::from_provisioned_bytes(DIAGNOSTIC_TEST_SECRET.as_bytes()).unwrap();
    let invocation_id = Uuid::new_v4();
    let mut child = spawn_fake_protocol_child(fixture, arm);
    let mut supervisor = child.stdin.take().unwrap();
    write_supervisor_bootstrap(&mut supervisor, &plan, invocation_id, &secret).unwrap();
    supervisor.flush().unwrap();
    BootstrappedFakeProtocol {
        child,
        supervisor,
        plan_bytes,
        sanitized_plan_sha256,
        invocation_id,
    }
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("fixture with spaces");
        std::fs::create_dir(&root).unwrap();
        let driver = PathBuf::from(env!("CARGO_BIN_EXE_distributed-state-ab-driver"))
            .canonicalize()
            .unwrap();
        let observer = PathBuf::from(env!("CARGO_BIN_EXE_distributed-state-ab-observer"))
            .canonicalize()
            .unwrap();
        let server = write_fixture(&root, "server.bin", b"shared-cd-server");
        let trace = write_fixture(&root, "trace.json", b"{\"records\":80000}");
        let config = write_fixture(&root, "config.json", b"{\"redacted\":true}");
        let dependencies = write_fixture(&root, "dependencies.json", b"{\"images\":[]}");
        let virtual_script = write_fixture(
            &root,
            "virtual-control.json",
            b"{\"outcomes\":\"fixed-success\"}",
        );
        let protocol = write_fixture(&root, "protocol.md", b"cycle-60-protocol-v1");
        let manifest = ManifestV1 {
            schema_version: MANIFEST_SCHEMA.to_owned(),
            notice: NOTICE.to_owned(),
            execution_eligible: false,
            purpose: "engineering_instrumentation_ab_nonfeedback_dry_run".to_owned(),
            attempt_id: "cycle62-dry-run".to_owned(),
            artifacts: ArtifactSetV1 {
                driver: artifact(&driver),
                observer: artifact(&observer),
                server: artifact(&server),
                trace_manifest: artifact(&trace),
                redacted_config: artifact(&config),
                dependency_manifest: artifact(&dependencies),
                virtual_control_script: artifact(&virtual_script),
                protocol_spec: artifact(&protocol),
            },
            node_ordinals: [0, 1, 2],
            workload: WorkloadV1 {
                record_count: 80_000,
                records_per_second: 400,
                input_target_end_ns: 200_000_000_000,
            },
            observer_policy: ObserverPolicyV1 {
                poll_interval_ns: 5_000_000_000,
                local_evidence_max_bytes: 4 * 1_024,
                exact_timing_max_bytes: 64 * 1_024,
            },
            limits: LimitsV1 {
                max_driver_actions: 104,
                max_observer_slots: 58,
                observer_stdout_max_bytes: 512 * 1_024,
                observer_stderr_max_bytes: 16 * 1_024,
                observer_completion_timeout_ms: 2_000,
            },
            pair: PairV1 {
                control: ObserverMode::Suppress,
                treatment: ObserverMode::Poll,
                shared_server: true,
                shared_config: true,
            },
            authentication: AuthenticationV1::SyntheticNone,
        };
        let manifest_path = root.join("manifest.json");
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        Self {
            _directory: directory,
            root,
            driver,
            manifest,
            manifest_path,
        }
    }

    fn run(&self, arm: &str, behavior: &str, name: &str) -> Run {
        let artifact_directory = self.root.join(name);
        let output = Command::new(&self.driver)
            .arg("dry-run")
            .arg(&self.manifest_path)
            .arg(&artifact_directory)
            .arg(arm)
            .arg(behavior)
            .env("LAMINAR_AB_SECRET_SENTINEL", SECRET_SENTINEL)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let record: DryRunRecordV1 = serde_json::from_slice(
            &std::fs::read(artifact_directory.join("dry-run-record.json")).unwrap(),
        )
        .unwrap();
        let plan = std::fs::read(artifact_directory.join("base-plan.json")).unwrap();
        let trace = std::fs::read(artifact_directory.join("driver-trace.json")).unwrap();
        let observer_stdout = std::fs::read(artifact_directory.join("observer.stdout")).unwrap();
        let observer_stderr = std::fs::read(artifact_directory.join("observer.stderr")).unwrap();
        let mut artifact_bytes = Vec::new();
        for entry in std::fs::read_dir(&artifact_directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                artifact_bytes.extend(std::fs::read(path).unwrap());
            }
        }
        Run {
            output,
            record,
            plan,
            trace,
            observer_stdout,
            observer_stderr,
            artifact_bytes,
        }
    }

    fn run_fake_driver(
        &self,
        arm: &str,
        behavior: &str,
        name: &str,
        addresses: [SocketAddr; 3],
        input: DriverFakeInput,
    ) -> FakeDriverRun {
        let artifact_directory = self.root.join(name);
        let mut child = Command::new(&self.driver)
            .arg("fake-protocol")
            .arg(&self.manifest_path)
            .arg(&artifact_directory)
            .arg(arm)
            .arg(behavior)
            .env("LAMINAR_AB_SECRET_SENTINEL", SECRET_SENTINEL)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = Some(child.stdin.take().unwrap());
        match input {
            DriverFakeInput::Valid(secret) => {
                let secret =
                    DiagnosticReadSecret::from_provisioned_bytes(secret.as_bytes()).unwrap();
                write_driver_fake_input(stdin.as_mut().unwrap(), addresses, &secret).unwrap();
            }
            DriverFakeInput::Invalid => {
                let mut pipe = stdin.take().unwrap();
                pipe.write_all(b"invalid-driver-frame").unwrap();
                drop(pipe);
            }
            DriverFakeInput::HeldOpen => {}
            DriverFakeInput::PartialHeldOpen => {
                let secret =
                    DiagnosticReadSecret::from_provisioned_bytes(DIAGNOSTIC_TEST_SECRET.as_bytes())
                        .unwrap();
                let mut frame = Vec::new();
                write_driver_fake_input(&mut frame, addresses, &secret).unwrap();
                let secret_start = frame.len() - DIAGNOSTIC_TEST_SECRET.len() - 1;
                stdin
                    .as_mut()
                    .unwrap()
                    .write_all(&frame[..secret_start + 11])
                    .unwrap();
                frame.zeroize();
            }
        }
        let output = wait_for_child(child, CHILD_COMPLETION_BOUND);
        drop(stdin);
        for secret in [
            SECRET_SENTINEL.as_bytes(),
            DIAGNOSTIC_TEST_SECRET.as_bytes(),
            WRONG_DIAGNOSTIC_TEST_SECRET.as_bytes(),
        ] {
            assert!(!output
                .stdout
                .windows(secret.len())
                .any(|window| window == secret));
            assert!(!output
                .stderr
                .windows(secret.len())
                .any(|window| window == secret));
        }
        let record: FakeProtocolRunRecordV1 = serde_json::from_slice(
            &std::fs::read(artifact_directory.join("fake-protocol-run-record.json")).unwrap(),
        )
        .unwrap();
        let plan = std::fs::read(artifact_directory.join("base-plan.json")).unwrap();
        let trace = std::fs::read(artifact_directory.join("driver-trace.json")).unwrap();
        let mut artifact_bytes = Vec::new();
        let mut artifact_names = Vec::new();
        for entry in std::fs::read_dir(&artifact_directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                artifact_names.push(path.file_name().unwrap().to_string_lossy().into_owned());
                artifact_bytes.extend(std::fs::read(path).unwrap());
            }
        }
        artifact_names.sort();
        for secret in [
            SECRET_SENTINEL.as_bytes(),
            DIAGNOSTIC_TEST_SECRET.as_bytes(),
            WRONG_DIAGNOSTIC_TEST_SECRET.as_bytes(),
        ] {
            assert!(!artifact_bytes
                .windows(secret.len())
                .any(|window| window == secret));
        }
        FakeDriverRun {
            output,
            record,
            plan,
            trace,
            artifact_bytes,
            artifact_names,
        }
    }
}

fn write_fixture(root: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = root.join(name);
    std::fs::write(&path, bytes).unwrap();
    path.canonicalize().unwrap()
}

fn artifact(path: &Path) -> ArtifactIdentityV1 {
    let bytes = std::fs::read(path).unwrap();
    ArtifactIdentityV1 {
        path: path.canonicalize().unwrap(),
        byte_length: bytes.len() as u64,
        sha256: sha256_bytes(&bytes),
    }
}

fn fixture_hash(path: &Path) -> String {
    sha256_bytes(&std::fs::read(path).unwrap())
}

#[test]
fn observer_outcomes_and_arms_cannot_change_the_driver_trace() {
    let fixture = Fixture::new();
    let mut expected_plan = None;
    let mut expected_trace = None;
    let mut expected_plan_hash = None;
    let mut expected_trace_hash = None;
    let mut expected_artifact_digests = None;
    let mut control_result = None;
    let mut treatment_result = None;

    for arm in ["C", "D"] {
        for behavior in ["success", "exit", "hang", "malformed"] {
            let run = fixture.run(arm, behavior, &format!("run-{arm}-{behavior}"));
            assert!(String::from_utf8_lossy(&run.output.stdout).starts_with(NOTICE));
            assert!(!String::from_utf8_lossy(&run.output.stdout).contains(SECRET_SENTINEL));
            assert!(!String::from_utf8_lossy(&run.output.stderr).contains(SECRET_SENTINEL));
            assert!(!run
                .artifact_bytes
                .windows(SECRET_SENTINEL.len())
                .any(|window| window == SECRET_SENTINEL.as_bytes()));
            assert!(run.record.trace_matches_plan);
            assert!(run.record.observer_outcome_consumed_only_after_end);
            assert_eq!(run.record.action_count, 104);
            assert_eq!(run.record.scheduled_end_ns, 290_000_000_000);
            assert!(!run.record.execution_eligible);
            assert_eq!(run.record.notice, NOTICE);
            assert_eq!(run.record.injected_observer_behavior, behavior);
            assert!(run.record.observer_pid.is_some());
            let expected_disposition = match behavior {
                "success" => ObserverDispositionV1::Valid,
                "exit" => ObserverDispositionV1::ExitNonzero,
                "hang" => ObserverDispositionV1::HungKilled,
                "malformed" => ObserverDispositionV1::Malformed,
                _ => unreachable!(),
            };
            assert_eq!(run.record.observer_disposition, expected_disposition);

            let end_index = run
                .record
                .supervisor_events
                .iter()
                .position(|event| event == "end_seal_consumed")
                .unwrap();
            for (index, event) in run.record.supervisor_events.iter().enumerate() {
                if event.contains("status")
                    || event.contains("killed")
                    || event.contains("capture_consumed")
                {
                    assert!(index > end_index, "{event:?} occurred before end seal");
                }
            }

            match &expected_plan {
                Some(expected) => assert_eq!(&run.plan, expected),
                None => expected_plan = Some(run.plan.clone()),
            }
            match &expected_trace {
                Some(expected) => assert_eq!(&run.trace, expected),
                None => expected_trace = Some(run.trace.clone()),
            }
            match &expected_plan_hash {
                Some(expected) => assert_eq!(&run.record.base_plan_sha256, expected),
                None => expected_plan_hash = Some(run.record.base_plan_sha256.clone()),
            }
            match &expected_trace_hash {
                Some(expected) => assert_eq!(&run.record.driver_trace_sha256, expected),
                None => expected_trace_hash = Some(run.record.driver_trace_sha256.clone()),
            }
            match &expected_artifact_digests {
                Some(expected) => assert_eq!(&run.record.artifact_digests, expected),
                None => expected_artifact_digests = Some(run.record.artifact_digests.clone()),
            }

            if behavior == "success" {
                let result: ObserverResultV1 =
                    serde_json::from_slice(&run.observer_stdout).unwrap();
                if arm == "C" {
                    control_result = Some(result);
                } else {
                    treatment_result = Some(result);
                }
            }
            assert_eq!(
                run.record.observer_stdout_retained_sha256,
                sha256_bytes(&run.observer_stdout)
            );
            assert_eq!(
                run.record.observer_stdout_retained_bytes as usize,
                run.observer_stdout.len()
            );
            assert_eq!(
                run.record.observer_stderr_retained_sha256,
                sha256_bytes(&run.observer_stderr)
            );
            assert_eq!(
                run.record.observer_stderr_retained_bytes as usize,
                run.observer_stderr.len()
            );
        }
    }

    let control = control_result.unwrap();
    let treatment = treatment_result.unwrap();
    assert_eq!(control.mode, ObserverMode::Suppress);
    assert_eq!(treatment.mode, ObserverMode::Poll);
    assert_eq!(
        control.observer_schedule_sha256,
        treatment.observer_schedule_sha256
    );
    assert_eq!(control.scheduled_slots, 58);
    assert_eq!(treatment.scheduled_slots, 58);
    assert_eq!(control.suppressed_probes, 348);
    assert!(control.planned_probes.is_empty());
    assert_eq!(treatment.suppressed_probes, 0);
    assert_eq!(treatment.planned_probes.len(), 348);
    let first = &treatment.planned_probes[0];
    assert_eq!(first.slot_ordinal, 0);
    assert_eq!(first.at_ns, 0);
    assert_eq!(first.boundary, Some(LifecycleBoundaryV1::WindowStart));
    assert_eq!(first.node_ordinal, 0);
    assert_eq!(first.route, DiagnosticRouteV1::LocalEvidence);
    let second = &treatment.planned_probes[1];
    assert_eq!(second.route, DiagnosticRouteV1::ExactTiming);
    let last = treatment.planned_probes.last().unwrap();
    assert_eq!(last.slot_ordinal, 57);
    assert_eq!(last.at_ns, 285_000_000_000);
    assert_eq!(last.boundary, None);
    assert_eq!(last.node_ordinal, 2);
    assert_eq!(last.route, DiagnosticRouteV1::ExactTiming);

    let plan: BasePlanV1 = serde_json::from_slice(expected_plan.as_ref().unwrap()).unwrap();
    assert_eq!(plan.driver_schedule.actions.len(), 104);
    assert_eq!(plan.observer_schedule.slots.len(), 58);
    assert_eq!(
        plan.source_manifest_sha256,
        fixture_hash(&fixture.manifest_path)
    );
    assert_eq!(
        Some(plan.artifact_digests.clone()),
        expected_artifact_digests
    );
    let boundaries: Vec<_> = plan
        .observer_schedule
        .slots
        .iter()
        .filter_map(|slot| slot.boundary.map(|boundary| (slot.at_ns, boundary)))
        .collect();
    assert_eq!(
        boundaries,
        vec![
            (0, LifecycleBoundaryV1::WindowStart),
            (120_000_000_000, LifecycleBoundaryV1::FaultCheckpoint),
            (200_000_000_000, LifecycleBoundaryV1::InputTargetEnd),
            (
                255_000_000_000,
                LifecycleBoundaryV1::PostRecoverySamplingAnchor
            ),
        ]
    );
}

#[test]
fn artifact_directory_is_exclusive_and_cannot_be_truncated() {
    let fixture = Fixture::new();
    let first = fixture.run("C", "success", "exclusive-run");
    let directory = fixture.root.join("exclusive-run");
    let before = sha256_bytes(&std::fs::read(directory.join("driver-trace.json")).unwrap());
    let second = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&fixture.manifest_path)
        .arg(&directory)
        .arg("D")
        .arg("success")
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("exclusive artifact directory"));
    assert_eq!(
        sha256_bytes(&std::fs::read(directory.join("driver-trace.json")).unwrap()),
        before
    );
    assert_eq!(
        first.record.observer_disposition,
        ObserverDispositionV1::Valid
    );
}

#[test]
fn minimum_stdout_cap_accepts_the_official_treatment_output() {
    let mut fixture = Fixture::new();
    fixture.manifest.limits.observer_stdout_max_bytes = 256 * 1_024;
    std::fs::write(
        &fixture.manifest_path,
        serde_json::to_vec(&fixture.manifest).unwrap(),
    )
    .unwrap();
    let run = fixture.run("D", "success", "minimum-cap-run");
    assert_eq!(
        run.record.observer_disposition,
        ObserverDispositionV1::Valid
    );
    assert!(run.observer_stdout.len() <= 256 * 1_024);
}

#[test]
fn manifest_rejects_credentials_unknown_fields_and_excessive_policy_before_spawn() {
    let fixture = Fixture::new();
    let original: Value =
        serde_json::from_slice(&std::fs::read(&fixture.manifest_path).unwrap()).unwrap();

    let mut token = original.clone();
    token["token_file"] = Value::String("forbidden-secret".to_owned());
    let token_path = fixture.root.join("manifest-token.json");
    std::fs::write(&token_path, serde_json::to_vec(&token).unwrap()).unwrap();
    let token_output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&token_path)
        .arg(fixture.root.join("token-run"))
        .arg("C")
        .arg("success")
        .output()
        .unwrap();
    assert!(!token_output.status.success());
    assert!(!fixture.root.join("token-run").exists());

    let mut excessive = original;
    excessive["observer_policy"]["local_evidence_max_bytes"] = Value::from(4097);
    let excessive_path = fixture.root.join("manifest-excessive.json");
    std::fs::write(&excessive_path, serde_json::to_vec(&excessive).unwrap()).unwrap();
    let excessive_output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&excessive_path)
        .arg(fixture.root.join("excessive-run"))
        .arg("D")
        .arg("success")
        .output()
        .unwrap();
    assert!(!excessive_output.status.success());
    assert!(String::from_utf8_lossy(&excessive_output.stderr)
        .contains("local_evidence_max_bytes must be in 1..=4096"));
    assert!(!fixture.root.join("excessive-run").exists());
}

#[test]
fn changed_input_artifact_fails_before_artifact_directory_creation() {
    let fixture = Fixture::new();
    std::fs::write(
        &fixture.manifest.artifacts.redacted_config.path,
        b"{\"redacted\":false}",
    )
    .unwrap();
    let run_directory = fixture.root.join("substitution-run");
    let output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&fixture.manifest_path)
        .arg(&run_directory)
        .arg("C")
        .arg("success")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("identity mismatch"));
    assert!(!run_directory.exists());
}

#[test]
fn observer_spawn_failure_is_classified_only_after_the_same_trace_completes() {
    let fixture = Fixture::new();
    let baseline = fixture.run("D", "success", "spawn-baseline");
    let fake_observer = write_fixture(&fixture.root, "not-an-executable.txt", b"not executable");
    let mut manifest = fixture.manifest.clone();
    manifest.attempt_id = "cycle62-spawn-failure".to_owned();
    manifest.artifacts.observer = artifact(&fake_observer);
    let manifest_path = fixture.root.join("manifest-spawn-failure.json");
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let artifact_directory = fixture.root.join("spawn-failure-run");
    let output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&manifest_path)
        .arg(&artifact_directory)
        .arg("D")
        .arg("success")
        .output()
        .unwrap();
    assert!(output.status.success());
    let record: DryRunRecordV1 = serde_json::from_slice(
        &std::fs::read(artifact_directory.join("dry-run-record.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        record.observer_disposition,
        ObserverDispositionV1::SpawnFailed
    );
    assert_eq!(record.observer_pid, None);
    assert!(record.observer_outcome_consumed_only_after_end);
    assert_eq!(
        std::fs::read(artifact_directory.join("driver-trace.json")).unwrap(),
        baseline.trace
    );
}

#[test]
fn fake_driver_consumes_complete_results_only_after_the_same_end_seal() {
    let fixture = Fixture::new();
    let (_listeners, control_addresses) = passive_loopback_endpoints();
    let control = fixture.run_fake_driver(
        "C",
        "success",
        "fake-driver-control-complete",
        control_addresses,
        DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
    );
    let servers = [
        OwnedFakeServer::success(1),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    let treatment_addresses = std::array::from_fn(|index| servers[index].address);
    let treatment = fixture.run_fake_driver(
        "D",
        "success",
        "fake-driver-treatment-complete",
        treatment_addresses,
        DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
    );

    for run in [&control, &treatment] {
        assert!(run.output.status.success());
        assert_eq!(
            run.record.observer_disposition,
            FakeProtocolObserverDispositionV1::Complete
        );
        assert!(run.record.observer_protocol_result.is_some());
        assert!(run.record.sanitized_observer_plan_sha256.is_some());
        assert_eq!(
            run.record.observer_invocation_id,
            Some(
                run.record
                    .observer_protocol_result
                    .as_ref()
                    .unwrap()
                    .invocation_id
            )
        );
        assert!(run.record.observer_outcome_consumed_only_after_end);
        assert!(!run.record.raw_observer_output_persisted);
        assert_eq!(run.record.action_count, 104);
        assert_eq!(run.record.scheduled_end_ns, 290_000_000_000);
        assert_eq!(
            run.artifact_names,
            [
                "base-plan.json",
                "driver-trace.json",
                "fake-protocol-run-record.json"
            ]
        );
        assert!(!run.artifact_bytes.is_empty());
        assert_fake_outcome_is_post_end(&run.record);
    }
    assert_eq!(control.plan, treatment.plan);
    assert_eq!(control.trace, treatment.trace);
    assert_ne!(
        control.record.observer_invocation_id,
        treatment.record.observer_invocation_id
    );
    let control_result = control.record.observer_protocol_result.as_ref().unwrap();
    assert_eq!(control_result.suppressed_probes, 348);
    assert_eq!(control_result.connection_attempts, 0);
    let treatment_result = treatment.record.observer_protocol_result.as_ref().unwrap();
    assert_eq!(treatment_result.connection_attempts, 348);
    assert_eq!(treatment_result.parsed_responses, 348);
    for server in &servers {
        assert_eq!(server.connection_count(), 116);
    }
}

#[test]
fn fake_driver_failures_cannot_change_plan_trace_or_persist_raw_output() {
    let fixture = Fixture::new();
    let (_listeners, addresses) = passive_loopback_endpoints();
    let baseline = fixture.run_fake_driver(
        "C",
        "success",
        "fake-driver-failure-baseline",
        addresses,
        DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
    );
    let mut runs = vec![
        (
            fixture.run_fake_driver(
                "C",
                "exit",
                "fake-driver-early-exit",
                addresses,
                DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
            ),
            FakeProtocolObserverDispositionV1::ExitNonzero,
        ),
        (
            fixture.run_fake_driver(
                "C",
                "hang",
                "fake-driver-hang",
                addresses,
                DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
            ),
            FakeProtocolObserverDispositionV1::CompletionDeadlineExceeded,
        ),
        (
            fixture.run_fake_driver(
                "C",
                "malformed",
                "fake-driver-malformed",
                addresses,
                DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
            ),
            FakeProtocolObserverDispositionV1::InvalidResult,
        ),
        (
            fixture.run_fake_driver(
                "C",
                "success",
                "fake-driver-invalid-input",
                addresses,
                DriverFakeInput::Invalid,
            ),
            FakeProtocolObserverDispositionV1::ProvisioningRejected,
        ),
        (
            fixture.run_fake_driver(
                "C",
                "success",
                "fake-driver-partial-held-input",
                addresses,
                DriverFakeInput::PartialHeldOpen,
            ),
            FakeProtocolObserverDispositionV1::ProvisioningRejected,
        ),
        (
            fixture.run_fake_driver(
                "C",
                "success",
                "fake-driver-held-input",
                addresses,
                DriverFakeInput::HeldOpen,
            ),
            FakeProtocolObserverDispositionV1::ProvisioningRejected,
        ),
    ];
    let incomplete_servers = [
        OwnedFakeServer::busy_then_success(1, 2),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    runs.push((
        fixture.run_fake_driver(
            "D",
            "success",
            "fake-driver-incomplete",
            std::array::from_fn(|index| incomplete_servers[index].address),
            DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
        ),
        FakeProtocolObserverDispositionV1::Incomplete,
    ));
    let stalled_servers = [
        OwnedFakeServer::stall(),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    runs.push((
        fixture.run_fake_driver(
            "D",
            "success",
            "fake-driver-cancelled",
            std::array::from_fn(|index| stalled_servers[index].address),
            DriverFakeInput::Valid(DIAGNOSTIC_TEST_SECRET),
        ),
        FakeProtocolObserverDispositionV1::CompletionDeadlineExceeded,
    ));
    let auth_servers = [
        OwnedFakeServer::success(1),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    runs.push((
        fixture.run_fake_driver(
            "D",
            "success",
            "fake-driver-wrong-authority",
            std::array::from_fn(|index| auth_servers[index].address),
            DriverFakeInput::Valid(WRONG_DIAGNOSTIC_TEST_SECRET),
        ),
        FakeProtocolObserverDispositionV1::ExitNonzero,
    ));

    for (run, expected) in runs {
        assert_eq!(run.output.status.code(), Some(2));
        assert_eq!(run.record.observer_disposition, expected);
        assert_eq!(run.plan, baseline.plan);
        assert_eq!(run.trace, baseline.trace);
        assert!(!run.record.raw_observer_output_persisted);
        assert_eq!(
            run.artifact_names,
            [
                "base-plan.json",
                "driver-trace.json",
                "fake-protocol-run-record.json"
            ]
        );
        assert_fake_outcome_is_post_end(&run.record);
        if expected == FakeProtocolObserverDispositionV1::Incomplete {
            assert_eq!(
                run.record
                    .observer_protocol_result
                    .as_ref()
                    .unwrap()
                    .disposition,
                ProtocolDispositionV2::Incomplete
            );
        } else {
            assert!(run.record.observer_protocol_result.is_none());
        }
    }
}

#[test]
fn fake_protocol_control_receives_only_the_typed_supervisor_bootstrap() {
    let fixture = Fixture::new();
    let listeners: [TcpListener; 3] = std::array::from_fn(|_| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        listener
    });
    let addresses = std::array::from_fn(|index| listeners[index].local_addr().unwrap());
    let BootstrappedFakeProtocol {
        child,
        supervisor,
        plan_bytes,
        sanitized_plan_sha256,
        invocation_id,
    } = spawn_bootstrapped_fake_protocol(&fixture, "C", addresses);
    assert!(!plan_bytes
        .windows(DIAGNOSTIC_TEST_SECRET.len())
        .any(|window| window == DIAGNOSTIC_TEST_SECRET.as_bytes()));

    let output = wait_for_child(child, BOOTSTRAP_FAILURE_BOUND);
    assert_observer_output_redacted(&output);
    assert!(output.status.success(), "observer status={}", output.status);
    let result: ObserverProtocolResultV3 = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result.invocation_id, invocation_id);
    assert_eq!(result.sanitized_plan_sha256, sanitized_plan_sha256);
    assert_eq!(result.suppressed_probes, 348);
    assert_eq!(result.connection_attempts, 0);
    assert_eq!(result.parsed_responses, 0);
    for listener in listeners {
        assert!(matches!(
            listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }
    drop(supervisor);
}

#[test]
fn fake_protocol_treatment_completes_with_open_supervisor_pipe_and_exact_connections() {
    let fixture = Fixture::new();
    let servers = [
        OwnedFakeServer::success(1),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    let addresses = std::array::from_fn(|index| servers[index].address);
    let BootstrappedFakeProtocol {
        child,
        supervisor,
        plan_bytes: _,
        sanitized_plan_sha256,
        invocation_id,
    } = spawn_bootstrapped_fake_protocol(&fixture, "D", addresses);

    let output = wait_for_child(child, CHILD_COMPLETION_BOUND);
    assert_observer_output_redacted(&output);
    let result: ObserverProtocolResultV3 = serde_json::from_slice(&output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    assert!(
        output.status.success(),
        "observer status={} result={result:?} stderr={stderr}",
        output.status
    );
    assert_eq!(result.sanitized_plan_sha256, sanitized_plan_sha256);
    assert_eq!(result.invocation_id, invocation_id);
    assert_eq!(result.disposition, ProtocolDispositionV2::Complete);
    assert_eq!(result.scheduled_slots, 58);
    assert_eq!(result.suppressed_probes, 0);
    assert_eq!(result.connection_attempts, 348);
    assert_eq!(result.parsed_responses, 348);
    assert_eq!(result.retries, 0);
    assert_eq!(result.transient_failures, 0);
    assert_eq!(result.process_transitions, 0);
    assert_eq!(result.timing_records, 0);
    assert_eq!(result.page_budget_deferrals, 0);
    assert_eq!(result.unresolved_timing_nodes, 0);
    assert_eq!(result.retained_events_dropped, 0);
    assert!(result.retained_events.is_empty());

    for (index, server) in servers.iter().enumerate() {
        assert_eq!(server.connection_count(), 116);
        let mut requests = server.drain_requests();
        assert_eq!(requests.len(), 116);
        let node_id = index as u64 + 1;
        let local = format!(
            "GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nAuthorization: Bearer {DIAGNOSTIC_TEST_SECRET}\r\nConnection: close\r\n\r\n",
            server.address
        );
        let timing = format!(
            "GET {TIMING_PATH}?after_sequence=0&expected_node_id={node_id}&expected_boot_incarnation={}&expected_process_term=1 HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nAuthorization: Bearer {DIAGNOSTIC_TEST_SECRET}\r\nConnection: close\r\n\r\n",
            boot_incarnation(node_id), server.address
        );
        let mut pairs = requests.chunks_exact(2);
        for pair in &mut pairs {
            assert!(pair[0] == local.as_bytes());
            assert!(pair[1] == timing.as_bytes());
        }
        assert!(pairs.remainder().is_empty());
        requests.zeroize();
    }
    drop(supervisor);
}

#[test]
fn fake_protocol_incomplete_collection_serializes_then_exits_nonzero() {
    let fixture = Fixture::new();
    let servers = [
        OwnedFakeServer::busy_then_success(1, 2),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    let addresses = std::array::from_fn(|index| servers[index].address);
    let BootstrappedFakeProtocol {
        child,
        supervisor,
        plan_bytes: _,
        sanitized_plan_sha256,
        invocation_id,
    } = spawn_bootstrapped_fake_protocol(&fixture, "D", addresses);

    let output = wait_for_child(child, CHILD_COMPLETION_BOUND);
    assert_observer_output_redacted(&output);
    assert_eq!(output.status.code(), Some(2));
    let result: ObserverProtocolResultV3 = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result.sanitized_plan_sha256, sanitized_plan_sha256);
    assert_eq!(result.invocation_id, invocation_id);
    assert_eq!(result.disposition, ProtocolDispositionV2::Incomplete);
    assert_eq!(result.connection_attempts, 348);
    assert_eq!(result.parsed_responses, 348);
    assert_eq!(result.retries, 1);
    assert_eq!(result.transient_failures, 1);
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("collection was incomplete"));
    let connection_counts: [usize; 3] =
        std::array::from_fn(|index| servers[index].connection_count());
    assert_eq!(connection_counts, [116, 116, 116]);
    drop(supervisor);
}

#[test]
fn fake_protocol_bootstrap_deadline_does_not_wait_for_supervisor_eof() {
    let fixture = Fixture::new();
    let mut child = spawn_fake_protocol_child(&fixture, "C");
    let stdin = child.stdin.take().unwrap();

    let output = wait_for_child(child, BOOTSTRAP_FAILURE_BOUND);
    assert_observer_output_redacted(&output);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("INVALID_OBSERVER_FAKE_PROTOCOL"));
    assert!(stderr.contains("bootstrap"));
    drop(stdin);
}

#[test]
fn fake_protocol_partial_secret_deadline_exits_the_dedicated_process() {
    let fixture = Fixture::new();
    let (_listeners, addresses) = passive_loopback_endpoints();
    let manifest = validate_manifest_path(&fixture.manifest_path).unwrap();
    let base_plan = seal_base_plan(&manifest).unwrap();
    let plan = build_sanitized_observer_plan(&base_plan, addresses).unwrap();
    let secret =
        DiagnosticReadSecret::from_provisioned_bytes(DIAGNOSTIC_TEST_SECRET.as_bytes()).unwrap();
    let mut frame = Vec::new();
    write_supervisor_bootstrap(&mut frame, &plan, Uuid::new_v4(), &secret).unwrap();
    let secret_start = frame.len() - DIAGNOSTIC_TEST_SECRET.len() - 1;
    let mut child = spawn_fake_protocol_child(&fixture, "C");
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&frame[..secret_start + 11]).unwrap();
    stdin.flush().unwrap();

    let output = wait_for_child(child, BOOTSTRAP_FAILURE_BOUND);
    frame.zeroize();
    assert_observer_output_redacted(&output);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("INVALID_OBSERVER_FAKE_PROTOCOL"));
    assert!(stderr.contains("bootstrap"));
    drop(stdin);
}

#[test]
fn fake_protocol_rejects_unsupported_environment_without_disclosing_it() {
    let fixture = Fixture::new();
    let mut command = fake_protocol_command(&fixture, "C");
    command.env(SECRET_SENTINEL, SECRET_SENTINEL);

    let output = wait_for_child(command.spawn().unwrap(), BOOTSTRAP_FAILURE_BOUND);
    assert_observer_output_redacted(&output);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("INVALID_OBSERVER_FAKE_PROTOCOL"));
    assert!(stderr.contains("environment contains an unsupported entry"));
}

#[test]
fn fake_protocol_cancel_frame_interrupts_a_stalled_probe() {
    let fixture = Fixture::new();
    let servers = [
        OwnedFakeServer::stall(),
        OwnedFakeServer::success(2),
        OwnedFakeServer::success(3),
    ];
    let addresses = std::array::from_fn(|index| servers[index].address);
    let BootstrappedFakeProtocol {
        child,
        supervisor: mut stdin,
        plan_bytes: _,
        sanitized_plan_sha256: _,
        invocation_id: _,
    } = spawn_bootstrapped_fake_protocol(&fixture, "D", addresses);
    wait_for_connections(&servers[0], 1, CANCELLATION_BOUND);
    stdin.write_all(SUPERVISOR_CANCEL_BYTES).unwrap();
    stdin.flush().unwrap();

    let output = wait_for_child(child, CANCELLATION_BOUND);
    assert_observer_output_redacted(&output);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("Cancelled"));
    let connection_counts: [usize; 3] =
        std::array::from_fn(|index| servers[index].connection_count());
    assert_eq!(connection_counts, [1, 0, 0]);
    drop(stdin);
}
