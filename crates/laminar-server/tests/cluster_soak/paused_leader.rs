//! Paused-leader fencing rounds for the three-node exactly-once soak.
//!
//! `kill -9` proves recovery from a dead leader; it never lets a former leader act again. These
//! rounds freeze a live leader with `SIGSTOP` inside a checkpoint gate, let the survivors elect a
//! successor and commit through the same failover assertions as a kill, and only then resume it
//! with `SIGCONT`. The stale leader must observe its lost identity lease and exit on its own
//! before the node restarts; the soak's frozen-input oracle must still find every output row
//! exactly once in Delta.
//!
//! The committed-index gate holds a prepared, quorum-certified checkpoint before its committed
//! index exists. The sink-commit gate holds a committed checkpoint after the leader read the
//! sink's committed cursor and before its external commit, so a resumed leader carries a stale
//! cursor. Unix only: the rounds signal processes with `kill`.

use std::io::{Read as _, Seek as _, SeekFrom};
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use super::{remove_marker, wait_for, JoinDelivery, Node};

const EXACT_SINK: &str = "soak_output";
const EXACT_GATES: [PauseGate; 2] = [PauseGate::CommittedIndex, PauseGate::SinkCommit(EXACT_SINK)];
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const LEASE_LOSS_MARKERS: [&str; 2] = [
    "node identity lease lost",
    "node identity lease expired or was superseded",
];

/// How a fault round takes the selected node out of the cluster.
#[derive(Clone, Copy, Debug)]
pub(super) enum RoundFault {
    /// `kill -9` inside the role's checkpoint gate.
    Kill9(&'static str),
    /// `SIGSTOP` the observed leader inside the gate; resumed after survivors fail over.
    PauseLeader(PauseGate),
}

/// Checkpoint phase at which a paused-leader round freezes the observed leader.
#[derive(Clone, Copy, Debug)]
pub(super) enum PauseGate {
    CommittedIndex,
    SinkCommit(&'static str),
}

/// Paused-leader rounds run after the kill rounds, only for cluster exactly-once.
pub(super) fn gates_for(delivery: JoinDelivery) -> &'static [PauseGate] {
    if cfg!(unix) && delivery == JoinDelivery::ExactlyOnce {
        &EXACT_GATES
    } else {
        &[]
    }
}

impl RoundFault {
    pub(super) fn arm(self, node: &Node) {
        match self {
            Self::Kill9(role) => node.arm_checkpoint_kill(role),
            Self::PauseLeader(gate) => gate.arm(node),
        }
    }

    pub(super) fn held(self, node: &Node) -> bool {
        match self {
            Self::Kill9(role) => node.checkpoint_kill_ready(role),
            Self::PauseLeader(gate) => gate.held(node).is_some(),
        }
    }

    /// Take the held node out of the cluster; a paused node is returned for a later resume.
    pub(super) fn deliver(self, node: &mut Node) -> Option<PausedNode> {
        match self {
            Self::Kill9(_) => {
                node.kill9();
                node.disarm_checkpoint_kill();
                None
            }
            Self::PauseLeader(gate) => {
                let held = gate.held(node).unwrap_or_default();
                let paused = node.pause();
                // Disarm only after the stop, so resume proceeds straight out of the gate.
                gate.disarm(node);
                eprintln!("soak: node{} paused in {gate:?}: {}", node.id, held.trim());
                Some(paused)
            }
        }
    }
}

impl PauseGate {
    pub(super) fn arm(self, node: &Node) {
        match self {
            Self::CommittedIndex => node.arm_checkpoint_kill("leader"),
            Self::SinkCommit(sink) => {
                let path = sink_gate_path(node);
                remove_marker(path);
                remove_marker(&path.with_extension("ready"));
                std::fs::write(path, format!("before:{sink}")).expect("arm sink commit gate");
            }
        }
    }

    /// The gate's ready record once the node is held inside it.
    pub(super) fn held(self, node: &Node) -> Option<String> {
        match self {
            Self::CommittedIndex => node
                .checkpoint_kill_ready("leader")
                .then(|| "leader before committed index".to_owned()),
            Self::SinkCommit(sink) => {
                let ready =
                    std::fs::read_to_string(sink_gate_path(node).with_extension("ready")).ok()?;
                (ready.split_ascii_whitespace().next() == Some(sink)).then_some(ready)
            }
        }
    }

    pub(super) fn disarm(self, node: &Node) {
        match self {
            Self::CommittedIndex => node.disarm_checkpoint_kill(),
            Self::SinkCommit(_) => {
                let path = sink_gate_path(node);
                remove_marker(path);
                remove_marker(&path.with_extension("ready"));
            }
        }
    }
}

fn sink_gate_path(node: &Node) -> &std::path::Path {
    node.sink_commit_gate_path
        .as_deref()
        .unwrap_or_else(|| panic!("node{} has no sink commit gate", node.id))
}

/// A `SIGSTOP`ped node process removed from the live set; dropping it kills the process.
pub(super) struct PausedNode {
    id: usize,
    child: Option<Child>,
    log_path: std::path::PathBuf,
    log_offset: u64,
}

impl Node {
    /// Freeze this node and remove it from the live set the soak helpers observe.
    pub(super) fn pause(&mut self) -> PausedNode {
        let child = self
            .child
            .take()
            .unwrap_or_else(|| panic!("node{} has no process to pause", self.id));
        let pid = child.id();
        let paused = PausedNode {
            id: self.id,
            child: Some(child),
            log_path: self.log_path.clone(),
            log_offset: std::fs::metadata(&self.log_path).map_or(0, |metadata| metadata.len()),
        };
        signal(pid, "STOP");
        wait_for(
            &format!("node{} pid {pid} to stop", self.id),
            STOP_TIMEOUT,
            || process_state(pid).starts_with('T'),
        );
        paused
    }
}

impl PausedNode {
    /// Resume the stale leader and require it to fence itself and exit within `window`.
    pub(super) fn resume_until_fenced_exit(mut self, window: Duration) -> ExitStatus {
        let child = self.child.as_mut().expect("paused node keeps its process");
        let pid = child.id();
        signal(pid, "CONT");
        let deadline = Instant::now() + window;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Ok(None) => panic!(
                    "resumed stale leader node{} pid {pid} did not fence itself and exit within \
                     {window:?}; log since pause:\n{}",
                    self.id,
                    self.log_since_pause()
                ),
                Err(error) => panic!(
                    "failed to inspect resumed node{} pid {pid}: {error}",
                    self.id
                ),
            }
        };
        self.child = None;
        assert_self_exit(self.id, pid, status);
        let log = self.log_since_pause();
        assert!(
            log.lines().any(|line| LEASE_LOSS_MARKERS
                .iter()
                .any(|marker| line.contains(marker))),
            "resumed stale leader node{} exited ({status}) without observing its lost identity \
             lease; log since pause:\n{log}",
            self.id
        );
        for line in log.lines().filter(|line| line.contains("fenc")).take(8) {
            eprintln!(
                "soak: stale leader node{} fencing evidence: {line}",
                self.id
            );
        }
        status
    }

    fn log_since_pause(&self) -> String {
        let Ok(mut log) = std::fs::File::open(&self.log_path) else {
            return String::new();
        };
        let mut bytes = Vec::new();
        if log.seek(SeekFrom::Start(self.log_offset)).is_err()
            || log.read_to_end(&mut bytes).is_err()
        {
            return String::new();
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl Drop for PausedNode {
    fn drop(&mut self) {
        // SIGKILL also terminates a stopped process, so an unwinding round leaves no frozen node.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(unix)]
fn assert_self_exit(id: usize, pid: u32, status: ExitStatus) {
    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(
        status.signal(),
        None,
        "resumed stale leader node{id} pid {pid} was terminated by a signal: {status}"
    );
}

#[cfg(not(unix))]
fn assert_self_exit(_id: usize, _pid: u32, _status: ExitStatus) {}

fn signal(pid: u32, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .unwrap_or_else(|error| panic!("failed to run kill -{name} {pid}: {error}"));
    assert!(status.success(), "kill -{name} {pid} failed: {status}");
}

fn process_state(pid: u32) -> String {
    Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}
