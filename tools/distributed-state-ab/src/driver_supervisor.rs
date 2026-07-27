use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use distributed_state_ab::observer_protocol::{
    bind_observer_protocol_plan, build_sanitized_observer_plan, validate_observer_protocol_result,
    write_supervisor_bootstrap, ObserverProtocolResultExpectationV3, ObserverProtocolResultV3,
    ProtocolDispositionV2, ProvisionedDriverFakeInput, PROTOCOL_RESULT_MAX_BYTES,
    SUPERVISOR_CANCEL_BYTES,
};
use distributed_state_ab::{
    sha256_bytes, validate_observer_result, verify_resolved_artifact, Arm,
    FakeProtocolObserverDispositionV1, ObserverBehavior, ObserverDispositionV1, SealedPlanV1,
    ValidatedManifestV1, START_SIGNAL_BYTES,
};
use uuid::Uuid;
use zeroize::Zeroize as _;

use super::persisted_end_seal::{EndBinding, PersistedEndSeal};

const OBSERVER_STDOUT_FILE: &str = "observer.stdout";
const OBSERVER_STDERR_FILE: &str = "observer.stderr";
const CAPTURE_COMPLETION_TIMEOUT: Duration = Duration::from_millis(250);
const REAP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const REAP_TIMEOUT: Duration = Duration::from_millis(500);
const FAKE_BOOTSTRAP_DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
const FAKE_CANCELLATION_WRITE_TIMEOUT: Duration = Duration::from_millis(250);
const FAKE_CANCELLATION_GRACE: Duration = Duration::from_millis(500);

enum ChildState {
    Spawned(SpawnedObserver),
    SpawnFailed,
}

struct SpawnedObserver {
    child: Child,
    pid: u32,
    stdout: Receiver<Capture>,
    stderr: Receiver<Capture>,
    start_signal_failed: bool,
}

struct Capture {
    bytes: Vec<u8>,
    oversized: bool,
    io_failed: bool,
}

pub(super) struct ObserverSupervisor {
    state: Option<ChildState>,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    events: Vec<String>,
}

pub(super) struct ObserverCollection {
    pub(super) disposition: ObserverDispositionV1,
    pub(super) outcome_consumed_only_after_end: bool,
    pub(super) events: Vec<String>,
    pub(super) end: EndBinding,
    pub(super) stdout_bytes: u32,
    pub(super) stdout_sha256: String,
    pub(super) stderr_bytes: u32,
    pub(super) stderr_sha256: String,
    pub(super) pid: Option<u32>,
}

enum FakeChildState {
    ProvisioningRejected,
    SpawnFailed {
        sanitized_plan_sha256: String,
        invocation_id: Uuid,
    },
    Spawned(Box<FakeSpawnedObserver>),
}

struct FakeSpawnedObserver {
    child: Child,
    pid: u32,
    stdout: Receiver<Capture>,
    stderr: Receiver<Capture>,
    input_pump: Option<FakeInputPump>,
    expectation: ObserverProtocolResultExpectationV3,
}

enum FakeInputCommand {
    Cancel,
    Close,
}

struct FakeInputPump {
    commands: SyncSender<FakeInputCommand>,
    bootstrap_delivery: Receiver<bool>,
    cancellation_delivery: Receiver<bool>,
    completion: Receiver<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

pub(super) struct FakeProtocolSupervisor {
    state: Option<FakeChildState>,
    events: Vec<String>,
}

pub(super) struct FakeProtocolCollection {
    pub(super) disposition: FakeProtocolObserverDispositionV1,
    pub(super) protocol_result: Option<ObserverProtocolResultV3>,
    pub(super) sanitized_plan_sha256: Option<String>,
    pub(super) invocation_id: Option<Uuid>,
    pub(super) outcome_consumed_only_after_end: bool,
    pub(super) events: Vec<String>,
    pub(super) end: EndBinding,
    pub(super) stdout_bytes: u32,
    pub(super) stdout_sha256: String,
    pub(super) stderr_bytes: u32,
    pub(super) stderr_sha256: String,
    pub(super) pid: Option<u32>,
}

struct FakeCollected {
    disposition: FakeProtocolObserverDispositionV1,
    protocol_result: Option<ObserverProtocolResultV3>,
    sanitized_plan_sha256: Option<String>,
    invocation_id: Option<Uuid>,
    stdout_bytes: u32,
    stdout_sha256: String,
    stderr_bytes: u32,
    stderr_sha256: String,
    pid: Option<u32>,
}

impl FakeCollected {
    fn empty(
        disposition: FakeProtocolObserverDispositionV1,
        sanitized_plan_sha256: Option<String>,
        invocation_id: Option<Uuid>,
    ) -> Self {
        Self {
            disposition,
            protocol_result: None,
            sanitized_plan_sha256,
            invocation_id,
            stdout_bytes: 0,
            stdout_sha256: sha256_bytes(&[]),
            stderr_bytes: 0,
            stderr_sha256: sha256_bytes(&[]),
            pid: None,
        }
    }
}

impl ObserverSupervisor {
    pub(super) fn spawn(
        manifest: &ValidatedManifestV1,
        manifest_path: &Path,
        artifact_directory: &Path,
        arm: Arm,
        behavior: ObserverBehavior,
    ) -> Self {
        let mut command = Command::new(&manifest.artifacts().observer.path);
        command
            .arg("dry-run")
            .arg(manifest_path)
            .arg(arm.label())
            .arg(behavior.label())
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut events = vec!["observer_spawn_attempted_stored".to_owned()];
        let state = match command.spawn() {
            Ok(mut child) => {
                events.push("observer_spawned_stored".to_owned());
                let pid = child.id();
                events.push(format!("observer_pid_stored={pid}"));
                let stdout = child
                    .stdout
                    .take()
                    .map(|pipe| {
                        capture_bounded(pipe, manifest.manifest().limits.observer_stdout_max_bytes)
                    })
                    .unwrap_or_else(disconnected_capture);
                let stderr = child
                    .stderr
                    .take()
                    .map(|pipe| {
                        capture_bounded(pipe, manifest.manifest().limits.observer_stderr_max_bytes)
                    })
                    .unwrap_or_else(disconnected_capture);
                let start_signal_failed = child.stdin.take().is_none_or(|mut stdin| {
                    stdin.write_all(START_SIGNAL_BYTES).is_err() || stdin.flush().is_err()
                });
                events.push(if start_signal_failed {
                    "observer_start_signal_failure_stored".to_owned()
                } else {
                    "observer_start_signal_sent_stored".to_owned()
                });
                ChildState::Spawned(SpawnedObserver {
                    child,
                    pid,
                    stdout,
                    stderr,
                    start_signal_failed,
                })
            }
            Err(_error) => {
                // Classification is deliberately unavailable until collect consumes EndSeal.
                events.push("observer_spawn_failure_stored".to_owned());
                ChildState::SpawnFailed
            }
        };
        Self {
            state: Some(state),
            stdout_path: artifact_directory.join(OBSERVER_STDOUT_FILE),
            stderr_path: artifact_directory.join(OBSERVER_STDERR_FILE),
            events,
        }
    }

    pub(super) fn collect(
        mut self,
        end_seal: PersistedEndSeal,
        manifest: &ValidatedManifestV1,
        plan: &SealedPlanV1,
        arm: Arm,
    ) -> Result<ObserverCollection, String> {
        let end = end_seal.consume_for(plan)?;
        self.events.push("end_seal_consumed".to_owned());
        let state = self
            .state
            .take()
            .ok_or_else(|| "observer state was already consumed".to_owned())?;
        let (mut disposition, stdout, stderr, pid) = match state {
            ChildState::SpawnFailed => (
                ObserverDispositionV1::SpawnFailed,
                Capture::empty(),
                Capture::empty(),
                None,
            ),
            ChildState::Spawned(mut observer) => {
                self.events
                    .push("observer_status_checked_post_end".to_owned());
                let wait = wait_after_end(
                    &mut observer.child,
                    Duration::from_millis(
                        manifest.manifest().limits.observer_completion_timeout_ms,
                    ),
                );
                let wait = if matches!(wait, WaitOutcome::TerminationFailed) {
                    self.events
                        .push("observer_cleanup_retry_post_end".to_owned());
                    terminate_and_reap(&mut observer.child)
                } else {
                    wait
                };
                let stdout = receive_capture(observer.stdout);
                let stderr = receive_capture(observer.stderr);
                self.events
                    .push("observer_capture_consumed_post_end".to_owned());
                let disposition = classify(
                    wait,
                    observer.start_signal_failed,
                    &stdout,
                    manifest,
                    plan,
                    arm,
                    &mut self.events,
                );
                (disposition, stdout, stderr, Some(observer.pid))
            }
        };

        if stdout.oversized || stderr.oversized {
            self.events
                .push("observer_output_oversized_post_end".to_owned());
            if disposition != ObserverDispositionV1::TerminationFailed {
                disposition = ObserverDispositionV1::OutputOversized;
            }
        } else if stdout.io_failed || stderr.io_failed {
            self.events
                .push("observer_capture_incomplete_post_end".to_owned());
            if disposition != ObserverDispositionV1::TerminationFailed {
                disposition = ObserverDispositionV1::CaptureIncomplete;
            }
        }

        write_new(&self.stdout_path, &stdout.bytes)?;
        write_new(&self.stderr_path, &stderr.bytes)?;
        let stdout_bytes = u32::try_from(stdout.bytes.len())
            .map_err(|_| "captured observer stdout length does not fit u32".to_owned())?;
        let stderr_bytes = u32::try_from(stderr.bytes.len())
            .map_err(|_| "captured observer stderr length does not fit u32".to_owned())?;
        Ok(ObserverCollection {
            disposition,
            outcome_consumed_only_after_end: true,
            events: std::mem::take(&mut self.events),
            end,
            stdout_bytes,
            stdout_sha256: sha256_bytes(&stdout.bytes),
            stderr_bytes,
            stderr_sha256: sha256_bytes(&stderr.bytes),
            pid,
        })
    }
}

impl FakeProtocolSupervisor {
    pub(super) fn spawn(
        manifest: &ValidatedManifestV1,
        manifest_path: &Path,
        arm: Arm,
        behavior: ObserverBehavior,
        base_plan: &SealedPlanV1,
        input: Option<ProvisionedDriverFakeInput>,
    ) -> Self {
        let mut events = Vec::new();
        let Some(input) = input else {
            events.push("observer_provisioning_rejection_stored".to_owned());
            return Self {
                state: Some(FakeChildState::ProvisioningRejected),
                events,
            };
        };
        let ProvisionedDriverFakeInput { addresses, secret } = input;
        let plan = match build_sanitized_observer_plan(base_plan, addresses) {
            Ok(plan) => plan,
            Err(_) => {
                events.push("observer_provisioning_rejection_stored".to_owned());
                return Self {
                    state: Some(FakeChildState::ProvisioningRejected),
                    events,
                };
            }
        };
        let invocation_id = Uuid::new_v4();
        let expectation = match bind_observer_protocol_plan(&plan, base_plan, arm, invocation_id) {
            Ok(expectation) => expectation,
            Err(_) => {
                events.push("observer_provisioning_rejection_stored".to_owned());
                return Self {
                    state: Some(FakeChildState::ProvisioningRejected),
                    events,
                };
            }
        };
        let sanitized_plan_sha256 = expectation.sanitized_plan_sha256().to_owned();
        events.push("observer_spawn_attempted_stored".to_owned());
        if verify_resolved_artifact("observer", &manifest.artifacts().observer).is_err() {
            events.push("observer_prespawn_identity_failure_stored".to_owned());
            return Self {
                state: Some(FakeChildState::SpawnFailed {
                    sanitized_plan_sha256,
                    invocation_id,
                }),
                events,
            };
        }
        let mut command = Command::new(&manifest.artifacts().observer.path);
        command
            .arg("fake-protocol")
            .arg(manifest_path)
            .arg(arm.label())
            .arg(behavior.label())
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(windows)]
        {
            let system_root = std::env::var_os("SystemRoot")
                .filter(|value| !value.is_empty() && std::path::Path::new(value).is_absolute());
            let Some(system_root) = system_root else {
                events.push("observer_system_root_rejection_stored".to_owned());
                return Self {
                    state: Some(FakeChildState::SpawnFailed {
                        sanitized_plan_sha256,
                        invocation_id,
                    }),
                    events,
                };
            };
            command.env("SystemRoot", system_root);
        }
        let state = match command.spawn() {
            Ok(mut child) => {
                events.push("observer_spawned_stored".to_owned());
                let pid = child.id();
                events.push(format!("observer_pid_stored={pid}"));
                let stdout = child
                    .stdout
                    .take()
                    .map(|pipe| capture_bounded(pipe, PROTOCOL_RESULT_MAX_BYTES as u32))
                    .unwrap_or_else(disconnected_capture);
                let stderr = child
                    .stderr
                    .take()
                    .map(|pipe| {
                        capture_bounded(pipe, manifest.manifest().limits.observer_stderr_max_bytes)
                    })
                    .unwrap_or_else(disconnected_capture);
                let input_pump = child.stdin.take().and_then(|stdin| {
                    start_fake_input_pump(stdin, plan, invocation_id, secret).ok()
                });
                events.push(if input_pump.is_some() {
                    "observer_bootstrap_pump_started_stored".to_owned()
                } else {
                    "observer_bootstrap_pump_failure_stored".to_owned()
                });
                FakeChildState::Spawned(Box::new(FakeSpawnedObserver {
                    child,
                    pid,
                    stdout,
                    stderr,
                    input_pump,
                    expectation,
                }))
            }
            Err(_) => {
                events.push("observer_spawn_failure_stored".to_owned());
                FakeChildState::SpawnFailed {
                    sanitized_plan_sha256,
                    invocation_id,
                }
            }
        };
        Self {
            state: Some(state),
            events,
        }
    }

    pub(super) fn collect(
        mut self,
        end_seal: PersistedEndSeal,
        manifest: &ValidatedManifestV1,
        plan: &SealedPlanV1,
    ) -> Result<FakeProtocolCollection, String> {
        let end = end_seal.consume_for(plan)?;
        self.events.push("end_seal_consumed".to_owned());
        let state = self
            .state
            .take()
            .ok_or_else(|| "observer state was already consumed".to_owned())?;
        let collected = match state {
            FakeChildState::ProvisioningRejected => {
                self.events
                    .push("observer_provisioning_rejection_classified_post_end".to_owned());
                FakeCollected::empty(
                    FakeProtocolObserverDispositionV1::ProvisioningRejected,
                    None,
                    None,
                )
            }
            FakeChildState::SpawnFailed {
                sanitized_plan_sha256,
                invocation_id,
            } => {
                self.events
                    .push("observer_spawn_failure_classified_post_end".to_owned());
                FakeCollected::empty(
                    FakeProtocolObserverDispositionV1::SpawnFailed,
                    Some(sanitized_plan_sha256),
                    Some(invocation_id),
                )
            }
            FakeChildState::Spawned(observer) => collect_fake_spawned(
                *observer,
                Duration::from_millis(manifest.manifest().limits.observer_completion_timeout_ms),
                &mut self.events,
            ),
        };
        Ok(FakeProtocolCollection {
            disposition: collected.disposition,
            protocol_result: collected.protocol_result,
            sanitized_plan_sha256: collected.sanitized_plan_sha256,
            invocation_id: collected.invocation_id,
            outcome_consumed_only_after_end: true,
            events: std::mem::take(&mut self.events),
            end,
            stdout_bytes: collected.stdout_bytes,
            stdout_sha256: collected.stdout_sha256,
            stderr_bytes: collected.stderr_bytes,
            stderr_sha256: collected.stderr_sha256,
            pid: collected.pid,
        })
    }
}

impl Drop for ObserverSupervisor {
    fn drop(&mut self) {
        if let Some(ChildState::Spawned(observer)) = &mut self.state {
            if matches!(
                bounded_cleanup(&mut observer.child),
                WaitOutcome::TerminationFailed
            ) {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "OBSERVER_CLEANUP_FAILED pid={} manual cleanup required",
                    observer.pid
                );
            }
        }
    }
}

impl Drop for FakeProtocolSupervisor {
    fn drop(&mut self) {
        if let Some(FakeChildState::Spawned(observer)) = &mut self.state {
            if let Some(pump) = &mut observer.input_pump {
                let _ = pump.request_cancel();
            }
            if matches!(
                bounded_cleanup(&mut observer.child),
                WaitOutcome::TerminationFailed
            ) {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "OBSERVER_CLEANUP_FAILED pid={} manual cleanup required",
                    observer.pid
                );
            }
            if let Some(mut pump) = observer.input_pump.take() {
                pump.close_and_join();
            }
        }
    }
}

fn start_fake_input_pump(
    mut stdin: ChildStdin,
    plan: distributed_state_ab::observer_protocol::SanitizedObserverPlanV2,
    invocation_id: Uuid,
    secret: distributed_state_ab::observer_protocol::DiagnosticReadSecret,
) -> Result<FakeInputPump, ()> {
    let (command_sender, command_receiver) = mpsc::sync_channel(1);
    let (bootstrap_sender, bootstrap_delivery) = mpsc::sync_channel(1);
    let (cancellation_sender, cancellation_delivery) = mpsc::sync_channel(1);
    let (completion_sender, completion) = mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("distributed-state-ab-observer-input".to_owned())
        .spawn(move || {
            let delivered =
                write_supervisor_bootstrap(&mut stdin, &plan, invocation_id, &secret).is_ok();
            drop(secret);
            let _ = bootstrap_sender.send(delivered);
            if delivered {
                match command_receiver.recv() {
                    Ok(FakeInputCommand::Cancel) => {
                        let cancelled = stdin
                            .write_all(SUPERVISOR_CANCEL_BYTES)
                            .and_then(|()| stdin.flush())
                            .is_ok();
                        let _ = cancellation_sender.send(cancelled);
                    }
                    Ok(FakeInputCommand::Close) | Err(_) => {}
                }
            }
            drop(stdin);
            let _ = completion_sender.send(());
        })
        .map_err(|_| ())?;
    Ok(FakeInputPump {
        commands: command_sender,
        bootstrap_delivery,
        cancellation_delivery,
        completion,
        thread: Some(thread),
    })
}

impl FakeInputPump {
    fn await_bootstrap_delivery(&self) -> Option<bool> {
        self.bootstrap_delivery
            .recv_timeout(FAKE_BOOTSTRAP_DELIVERY_TIMEOUT)
            .ok()
    }

    fn request_cancel(&self) -> bool {
        if self.commands.try_send(FakeInputCommand::Cancel).is_err() {
            return false;
        }
        self.cancellation_delivery
            .recv_timeout(FAKE_CANCELLATION_WRITE_TIMEOUT)
            .unwrap_or(false)
    }

    fn close_and_join(&mut self) {
        let _ = self.commands.try_send(FakeInputCommand::Close);
        if self
            .completion
            .recv_timeout(CAPTURE_COMPLETION_TIMEOUT)
            .is_ok()
        {
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        } else {
            let _ = self.thread.take();
        }
    }
}

fn collect_fake_spawned(
    mut observer: FakeSpawnedObserver,
    completion_timeout: Duration,
    events: &mut Vec<String>,
) -> FakeCollected {
    events.push("observer_status_checked_post_end".to_owned());
    let sanitized_plan_sha256 = Some(observer.expectation.sanitized_plan_sha256().to_owned());
    let invocation_id = Some(observer.expectation.invocation_id());
    let bootstrap_delivered = observer
        .input_pump
        .as_ref()
        .and_then(FakeInputPump::await_bootstrap_delivery)
        .unwrap_or(false);
    events.push(if bootstrap_delivered {
        "observer_bootstrap_delivery_confirmed_post_end".to_owned()
    } else {
        "observer_bootstrap_delivery_failure_post_end".to_owned()
    });
    let mut wait = wait_fake_after_end(
        &mut observer.child,
        completion_timeout,
        observer.input_pump.as_ref(),
        events,
    );
    if matches!(wait.outcome, WaitOutcome::TerminationFailed) {
        events.push("observer_cleanup_retry_post_end".to_owned());
        wait.outcome = terminate_and_reap(&mut observer.child);
        if matches!(wait.outcome, WaitOutcome::TerminationFailed) {
            events.push("observer_manual_cleanup_required_post_end".to_owned());
            let _ = writeln!(
                std::io::stderr().lock(),
                "OBSERVER_CLEANUP_FAILED pid={} manual cleanup required",
                observer.pid
            );
        }
    }
    if let Some(mut pump) = observer.input_pump.take() {
        pump.close_and_join();
    }
    let stdout = receive_capture(observer.stdout);
    let stderr = receive_capture(observer.stderr);
    events.push("observer_capture_consumed_post_end".to_owned());
    let stdout_bytes = u32::try_from(stdout.bytes.len()).expect("fake stdout cap fits u32");
    let stderr_bytes = u32::try_from(stderr.bytes.len()).expect("fake stderr cap fits u32");
    let stdout_sha256 = sha256_bytes(&stdout.bytes);
    let stderr_sha256 = sha256_bytes(&stderr.bytes);
    let (disposition, protocol_result) = classify_fake(
        wait,
        bootstrap_delivered,
        &stdout,
        &stderr,
        &observer.expectation,
        events,
    );
    FakeCollected {
        disposition,
        protocol_result,
        sanitized_plan_sha256,
        invocation_id,
        stdout_bytes,
        stdout_sha256,
        stderr_bytes,
        stderr_sha256,
        pid: Some(observer.pid),
    }
}

fn wait_fake_after_end(
    child: &mut Child,
    completion_timeout: Duration,
    input_pump: Option<&FakeInputPump>,
    events: &mut Vec<String>,
) -> FakeWaitResult {
    match poll_until_exit(child, completion_timeout) {
        PollOutcome::Exited(status) => {
            return FakeWaitResult {
                outcome: WaitOutcome::Exited(status),
                completion_deadline_missed: false,
                status_inspection_failed: false,
            };
        }
        PollOutcome::Failed => {
            events.push("observer_status_inspection_failed_post_end".to_owned());
            return FakeWaitResult {
                outcome: terminate_and_reap(child),
                completion_deadline_missed: false,
                status_inspection_failed: true,
            };
        }
        PollOutcome::TimedOut => {
            events.push("observer_completion_timeout_post_end".to_owned());
        }
    }
    let cancellation_frame_written = input_pump.is_some_and(FakeInputPump::request_cancel);
    events.push(if cancellation_frame_written {
        "observer_cancellation_frame_written_post_end".to_owned()
    } else {
        "observer_cancellation_write_failed_post_end".to_owned()
    });
    let (outcome, status_inspection_failed) = match poll_until_exit(child, FAKE_CANCELLATION_GRACE)
    {
        PollOutcome::Exited(status) => (WaitOutcome::Exited(status), false),
        PollOutcome::TimedOut => {
            events.push("observer_cancel_grace_exhausted_post_end".to_owned());
            (terminate_and_reap(child), false)
        }
        PollOutcome::Failed => {
            events.push("observer_status_inspection_failed_post_end".to_owned());
            (terminate_and_reap(child), true)
        }
    };
    FakeWaitResult {
        outcome,
        completion_deadline_missed: true,
        status_inspection_failed,
    }
}

struct FakeWaitResult {
    outcome: WaitOutcome,
    completion_deadline_missed: bool,
    status_inspection_failed: bool,
}

enum PollOutcome {
    Exited(ExitStatus),
    TimedOut,
    Failed,
}

fn poll_until_exit(child: &mut Child, timeout: Duration) -> PollOutcome {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return PollOutcome::Exited(status),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(REAP_POLL_INTERVAL),
            Ok(None) => return PollOutcome::TimedOut,
            Err(_) => return PollOutcome::Failed,
        }
    }
}

fn classify_fake(
    wait: FakeWaitResult,
    bootstrap_delivered: bool,
    stdout: &Capture,
    stderr: &Capture,
    expectation: &ObserverProtocolResultExpectationV3,
    events: &mut Vec<String>,
) -> (
    FakeProtocolObserverDispositionV1,
    Option<ObserverProtocolResultV3>,
) {
    if let Some(disposition) = fake_precondition_failure(&wait, bootstrap_delivered, stdout, stderr)
    {
        let event = match disposition {
            FakeProtocolObserverDispositionV1::TerminationFailed => {
                "observer_termination_failed_post_end"
            }
            FakeProtocolObserverDispositionV1::OutputOversized => {
                "observer_output_oversized_post_end"
            }
            FakeProtocolObserverDispositionV1::CaptureIncomplete => {
                "observer_capture_incomplete_post_end"
            }
            FakeProtocolObserverDispositionV1::BootstrapDeliveryFailed => {
                "observer_bootstrap_delivery_classified_post_end"
            }
            FakeProtocolObserverDispositionV1::CompletionDeadlineExceeded => {
                "observer_completion_deadline_classified_post_end"
            }
            FakeProtocolObserverDispositionV1::StatusInspectionFailed => {
                "observer_status_inspection_classified_post_end"
            }
            _ => unreachable!("precondition helper returns only fixed failures"),
        };
        events.push(event.to_owned());
        return (disposition, None);
    }
    match wait.outcome {
        WaitOutcome::KilledAndReaped => {
            unreachable!("fake kill/reap requires a sticky deadline or status failure")
        }
        WaitOutcome::Exited(status) => {
            let result = validate_observer_protocol_result(&stdout.bytes, expectation);
            match result {
                Ok(result)
                    if status.success()
                        && result.disposition == ProtocolDispositionV2::Complete =>
                {
                    events.push("observer_protocol_result_validated_post_end".to_owned());
                    (FakeProtocolObserverDispositionV1::Complete, Some(result))
                }
                Ok(result)
                    if status.code() == Some(2)
                        && result.disposition == ProtocolDispositionV2::Incomplete =>
                {
                    events.push("observer_protocol_result_validated_post_end".to_owned());
                    (FakeProtocolObserverDispositionV1::Incomplete, Some(result))
                }
                Ok(_) => (FakeProtocolObserverDispositionV1::InvalidResult, None),
                Err(_) if !status.success() => {
                    (FakeProtocolObserverDispositionV1::ExitNonzero, None)
                }
                Err(_) => (FakeProtocolObserverDispositionV1::InvalidResult, None),
            }
        }
        WaitOutcome::TerminationFailed => unreachable!("handled before classification"),
    }
}

fn fake_precondition_failure(
    wait: &FakeWaitResult,
    bootstrap_delivered: bool,
    stdout: &Capture,
    stderr: &Capture,
) -> Option<FakeProtocolObserverDispositionV1> {
    if matches!(wait.outcome, WaitOutcome::TerminationFailed) {
        Some(FakeProtocolObserverDispositionV1::TerminationFailed)
    } else if stdout.oversized || stderr.oversized {
        Some(FakeProtocolObserverDispositionV1::OutputOversized)
    } else if stdout.io_failed || stderr.io_failed {
        Some(FakeProtocolObserverDispositionV1::CaptureIncomplete)
    } else if !bootstrap_delivered {
        Some(FakeProtocolObserverDispositionV1::BootstrapDeliveryFailed)
    } else if wait.status_inspection_failed {
        Some(FakeProtocolObserverDispositionV1::StatusInspectionFailed)
    } else if wait.completion_deadline_missed {
        Some(FakeProtocolObserverDispositionV1::CompletionDeadlineExceeded)
    } else {
        None
    }
}

impl Capture {
    fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            oversized: false,
            io_failed: false,
        }
    }

    fn incomplete() -> Self {
        Self {
            bytes: Vec::new(),
            oversized: false,
            io_failed: true,
        }
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

fn capture_bounded(
    mut input: impl std::io::Read + Send + 'static,
    maximum: u32,
) -> Receiver<Capture> {
    let (sender, receiver) = mpsc::sync_channel(1);
    let _capture_thread = std::thread::Builder::new()
        .name("distributed-state-ab-capture".to_owned())
        .spawn(move || {
            let maximum = maximum as usize;
            let mut bytes = Vec::with_capacity(maximum.min(64 * 1_024));
            let mut oversized = false;
            let mut io_failed = false;
            let mut chunk = [0_u8; 8 * 1_024];
            loop {
                match input.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(count) => {
                        let remaining = maximum.saturating_sub(bytes.len());
                        let retained = count.min(remaining);
                        bytes.extend_from_slice(&chunk[..retained]);
                        oversized |= retained < count;
                    }
                    Err(_) => {
                        io_failed = true;
                        break;
                    }
                }
            }
            chunk.zeroize();
            let _ = sender.send(Capture {
                bytes,
                oversized,
                io_failed,
            });
        });
    receiver
}

fn disconnected_capture() -> Receiver<Capture> {
    let (sender, receiver) = mpsc::sync_channel(1);
    drop(sender);
    receiver
}

fn receive_capture(receiver: Receiver<Capture>) -> Capture {
    receiver
        .recv_timeout(CAPTURE_COMPLETION_TIMEOUT)
        .unwrap_or_else(|_| Capture::incomplete())
}

enum WaitOutcome {
    Exited(ExitStatus),
    KilledAndReaped,
    TerminationFailed,
}

fn wait_after_end(child: &mut Child, timeout: Duration) -> WaitOutcome {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return WaitOutcome::Exited(status),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(REAP_POLL_INTERVAL),
            Ok(None) | Err(_) => return terminate_and_reap(child),
        }
    }
}

fn terminate_and_reap(child: &mut Child) -> WaitOutcome {
    let deadline = Instant::now() + REAP_TIMEOUT;
    let mut kill_issued = false;
    loop {
        match child.try_wait() {
            Ok(Some(_)) if kill_issued => return WaitOutcome::KilledAndReaped,
            Ok(Some(status)) => return WaitOutcome::Exited(status),
            Ok(None) | Err(_) => {}
        }
        if child.kill().is_ok() {
            kill_issued = true;
        }
        if Instant::now() >= deadline {
            return match child.try_wait() {
                Ok(Some(_)) if kill_issued => WaitOutcome::KilledAndReaped,
                Ok(Some(status)) => WaitOutcome::Exited(status),
                Ok(None) | Err(_) => WaitOutcome::TerminationFailed,
            };
        }
        std::thread::sleep(REAP_POLL_INTERVAL);
    }
}

fn bounded_cleanup(child: &mut Child) -> WaitOutcome {
    if let Ok(Some(status)) = child.try_wait() {
        return WaitOutcome::Exited(status);
    }
    terminate_and_reap(child)
}

fn classify(
    wait: WaitOutcome,
    start_signal_failed: bool,
    stdout: &Capture,
    manifest: &ValidatedManifestV1,
    plan: &SealedPlanV1,
    arm: Arm,
    events: &mut Vec<String>,
) -> ObserverDispositionV1 {
    if matches!(wait, WaitOutcome::TerminationFailed) {
        events.push("observer_termination_failed_post_end".to_owned());
        return ObserverDispositionV1::TerminationFailed;
    }
    match wait {
        WaitOutcome::Exited(status) if !status.success() => ObserverDispositionV1::ExitNonzero,
        WaitOutcome::KilledAndReaped => {
            events.push("observer_killed_and_reaped_post_end".to_owned());
            ObserverDispositionV1::HungKilled
        }
        WaitOutcome::Exited(_) if start_signal_failed => ObserverDispositionV1::StartSignalFailed,
        WaitOutcome::Exited(_) => {
            if validate_observer_result(&stdout.bytes, manifest, plan, arm).is_ok() {
                ObserverDispositionV1::Valid
            } else {
                ObserverDispositionV1::Malformed
            }
        }
        WaitOutcome::TerminationFailed => ObserverDispositionV1::TerminationFailed,
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create artifact {}: {error}", path.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("write artifact {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync artifact {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_drains_input_but_retains_only_the_configured_cap() {
        let receiver = capture_bounded(std::io::Cursor::new(vec![7_u8; 32 * 1_024]), 1_024);
        let capture = receive_capture(receiver);
        assert_eq!(capture.bytes.len(), 1_024);
        assert!(capture.oversized);
        assert!(!capture.io_failed);
    }

    #[test]
    fn fake_precondition_failures_are_strictly_prioritized() {
        let wait = |outcome, completion_deadline_missed, status_inspection_failed| FakeWaitResult {
            outcome,
            completion_deadline_missed,
            status_inspection_failed,
        };
        let clean = Capture::empty();
        let oversized = Capture {
            bytes: vec![7],
            oversized: true,
            io_failed: false,
        };
        let incomplete = Capture::incomplete();
        assert_eq!(
            fake_precondition_failure(
                &wait(WaitOutcome::TerminationFailed, true, true),
                false,
                &oversized,
                &incomplete
            ),
            Some(FakeProtocolObserverDispositionV1::TerminationFailed)
        );
        assert_eq!(
            fake_precondition_failure(
                &wait(WaitOutcome::KilledAndReaped, false, false),
                true,
                &oversized,
                &clean
            ),
            Some(FakeProtocolObserverDispositionV1::OutputOversized)
        );
        assert_eq!(
            fake_precondition_failure(
                &wait(WaitOutcome::KilledAndReaped, false, false),
                true,
                &clean,
                &incomplete
            ),
            Some(FakeProtocolObserverDispositionV1::CaptureIncomplete)
        );
        assert_eq!(
            fake_precondition_failure(
                &wait(WaitOutcome::KilledAndReaped, false, false),
                false,
                &clean,
                &clean
            ),
            Some(FakeProtocolObserverDispositionV1::BootstrapDeliveryFailed)
        );
        assert_eq!(
            fake_precondition_failure(
                &wait(WaitOutcome::KilledAndReaped, true, false),
                true,
                &clean,
                &clean
            ),
            Some(FakeProtocolObserverDispositionV1::CompletionDeadlineExceeded)
        );
        assert_eq!(
            fake_precondition_failure(
                &wait(WaitOutcome::KilledAndReaped, false, true),
                true,
                &clean,
                &clean
            ),
            Some(FakeProtocolObserverDispositionV1::StatusInspectionFailed)
        );
    }

    #[test]
    fn bounded_cleanup_kills_and_reaps_a_parked_child() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "driver_supervisor::tests::cleanup_test_child_parks_forever",
                "--ignored",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert!(child.try_wait().unwrap().is_none());
        assert!(matches!(
            bounded_cleanup(&mut child),
            WaitOutcome::KilledAndReaped
        ));
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    #[ignore = "subprocess fixture invoked only by bounded_cleanup_kills_and_reaps_a_parked_child"]
    fn cleanup_test_child_parks_forever() {
        loop {
            std::thread::sleep(Duration::from_secs(60));
        }
    }
}
