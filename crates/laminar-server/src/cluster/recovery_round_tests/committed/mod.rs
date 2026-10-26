//! Independent database processes recover aggregates and process functions through the existing
//! shared checkpoint, assignment and recovery lifecycle.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::*;
use laminar_core::checkpoint::{CheckpointAssignmentFence, CommittedCheckpointRef};
use laminar_core::cluster::control::{TopologyAdmissionPhase, TopologyVersion};

mod child;
mod connectors;
mod migration;
mod process;
mod rotation;
mod scenario;

const CHILD_ENV: &str = "LAMINAR_DATABASE_RECOVERY_PEER";
const TEST_NAME: &str = "cluster::recovery_round_tests::committed::surviving_database_recovers_lost_vnodes_and_replays_the_committed_source_cut";
const PROCESS_TTL: Duration = Duration::from_secs(6);
const MAX_MESSAGE_BYTES: u64 = 64 * 1024;

#[derive(Serialize, Deserialize)]
struct PeerConfig {
    namespace: String,
    node: u64,
    assignment: AssignmentSnapshot,
    runtime: Runtime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Runtime {
    Aggregate,
    Native,
    #[cfg(feature = "process-remote")]
    RemoteRust,
}

#[derive(Serialize, Deserialize)]
enum Command {
    Connect(u64, std::net::SocketAddr),
    Catalog,
    Start,
    Prefix(u64),
    HoldRecovery,
    RemoveFailedPeer(u64),
    /// Mark a node `Draining`, so the leader moves its vnodes to the other node live.
    Drain(u64),
    ClearProcessObservation,
    Observe,
    Checkpoint,
    /// A committed read of one view: the newest cut, or exactly this epoch.
    ReadView(String, Option<u64>),
    /// Adopt the sealed bootstrap catalog as topology one; leader only.
    Adopt,
    /// Submit one migration of the committed topology; leader only.
    Migrate(u128, Vec<String>),
    Topology(u128),
    Release,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Resume {
    checkpoint_id: u64,
    assignment: u64,
    offsets: BTreeMap<String, String>,
    channels: Vec<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Observation {
    fenced: bool,
    polls: u64,
    starts: Vec<Resume>,
    output: BTreeMap<i64, i64>,
    assignment: Option<CheckpointAssignmentFence>,
    handoff: Option<CommittedCheckpointRef>,
    intent: Option<RecoveryAnnouncement>,
    release: Option<RecoveryAnnouncement>,
    fault: Option<String>,
    leader: bool,
    callbacks: Vec<process::Callback>,
    activity: Vec<process::ActivityRow>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Ready {
        pid: u32,
        address: std::net::SocketAddr,
    },
    Done,
    Observed(Box<Observation>),
    Committed {
        reference: CommittedCheckpointRef,
        offsets: BTreeMap<String, String>,
        participants: Vec<u64>,
        channels: Vec<Vec<u8>>,
        watermark: Option<i64>,
    },
    Rows(Vec<Vec<i64>>),
    Topology {
        phase: Option<TopologyAdmissionPhase>,
        committed: Option<TopologyVersion>,
        active: Option<TopologyVersion>,
    },
}

fn write_message(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if u64::try_from(bytes.len())? > MAX_MESSAGE_BYTES {
        return Err(anyhow!("fixture message exceeded its bound"));
    }
    let temporary = path.with_extension("pending");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn read_message<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_MESSAGE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > MAX_MESSAGE_BYTES {
        return Err(anyhow!("fixture message exceeded its bound"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires loopback MinIO; set LAMINAR_PROCESS_TEST_S3_ENDPOINT and LAMINAR_PROCESS_TEST_S3_BUCKET"]
async fn surviving_database_recovers_lost_vnodes_and_replays_the_committed_source_cut() {
    if let Some(root) = std::env::var_os(CHILD_ENV) {
        child::run(Path::new(&root)).await.unwrap();
        return;
    }
    scenario::run().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires loopback MinIO; set LAMINAR_PROCESS_TEST_S3_ENDPOINT and LAMINAR_PROCESS_TEST_S3_BUCKET"]
async fn two_databases_migrate_and_seed_views_from_the_cut() {
    scenario::run_migration().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires loopback MinIO; set LAMINAR_PROCESS_TEST_S3_ENDPOINT and LAMINAR_PROCESS_TEST_S3_BUCKET"]
async fn a_draining_database_hands_its_views_over_live() {
    scenario::run_drain().await.unwrap();
}
