use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use distributed_state_ab::{
    sha256_bytes, validate_observer_result, Arm, ObserverBehavior, ObserverDispositionV1,
    SealedPlanV1, ValidatedManifestV1, START_SIGNAL_BYTES,
};

use super::persisted_end_seal::{EndBinding, PersistedEndSeal};

const OBSERVER_STDOUT_FILE: &str = "observer.stdout";
const OBSERVER_STDERR_FILE: &str = "observer.stderr";
const CAPTURE_COMPLETION_TIMEOUT: Duration = Duration::from_millis(250);
const REAP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const REAP_TIMEOUT: Duration = Duration::from_millis(500);

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
