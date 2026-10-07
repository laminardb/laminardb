//! Independent graph owners over live S3 CAS; public DB admission remains rejected.
//!
//! The parent drives private graph hooks. This does not certify `RecoveryMonitor` intake gates,
//! independent source-channel merge order, or asynchronous timer rescheduling.

use super::replay::{CallbackIdentity, RecordingActivity};
use super::store::{capture, checkpoint_store, CheckpointWriter};
use super::*;
use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointBarrier, CheckpointStore, CommittedCheckpointRef,
};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

mod child;
mod scenario;

const CHILD_ENV: &str = "LAMINAR_PROCESS_PEER_ROOT";
const ENDPOINT_ENV: &str = "LAMINAR_PROCESS_TEST_S3_ENDPOINT";
const BUCKET_ENV: &str = "LAMINAR_PROCESS_TEST_S3_BUCKET";
const TEST_NAME: &str = "process_function::operator::execution::tests::shuffle::committed::peers::independent_owners_restore_the_committed_cut_after_host_loss_and_rescale";
const PEER_DEADLINE: Duration = Duration::from_secs(20);
const PEER_TTL: Duration = Duration::from_secs(6);
const MAX_MESSAGE_BYTES: u64 = 1024 * 1024;

type ActivityRow = (String, String, i64, bool, i64);

#[derive(Clone, Copy, Serialize, Deserialize)]
enum Runtime {
    Native,
    #[cfg(feature = "process-remote")]
    RemoteRust,
}

#[derive(Serialize, Deserialize)]
struct PeerConfig {
    namespace: String,
    node: u64,
    fence: CheckpointAssignmentFence,
    owners: [u64; 4],
    runtime: Runtime,
}

#[derive(Serialize, Deserialize)]
enum Command {
    Connect(Vec<(u64, SocketAddr)>),
    Input(Vec<(String, i64, i64)>),
    Advance(i64),
    Observe,
    Barrier,
    Capture(String),
    Pause,
    Restore {
        fence: CheckpointAssignmentFence,
        owners: [u64; 4],
        recovery: u64,
        reference: CommittedCheckpointRef,
    },
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Ready {
        pid: u32,
        address: SocketAddr,
    },
    Done,
    Observed {
        rows: Vec<ActivityRow>,
        callbacks: Vec<CallbackIdentity>,
        quiescent: bool,
        watermark_us: Option<i64>,
    },
    Captured {
        manifest: Box<laminar_core::checkpoint::CheckpointManifest>,
        encoded: Vec<u8>,
    },
    Restored {
        reference: CommittedCheckpointRef,
        reassigned: bool,
        offsets: std::collections::HashMap<String, String>,
        watermark: Option<i64>,
    },
    Rejected(String),
}

fn shared_objects(namespace: &str) -> Arc<dyn ObjectStore> {
    // Only the task's loopback MinIO fixture is admitted; never inherit cloud credentials.
    let endpoint = std::env::var(ENDPOINT_ENV).expect("set the local MinIO endpoint");
    let address: SocketAddr = endpoint
        .strip_prefix("http://")
        .expect("MinIO test requires an HTTP socket address")
        .parse()
        .unwrap();
    assert!(address.ip().is_loopback());
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(std::env::var(BUCKET_ENV).expect("set the MinIO test bucket"))
        .with_region("us-east-1")
        .with_access_key_id(
            std::env::var("LAMINAR_PROCESS_TEST_S3_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".into()),
        )
        .with_secret_access_key(
            std::env::var("LAMINAR_PROCESS_TEST_S3_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".into()),
        )
        .with_allow_http(true)
        .build()
        .unwrap();
    Arc::new(object_store::prefix::PrefixStore::new(store, namespace))
}

fn write_message(path: &Path, message: &impl Serialize) {
    let bytes = serde_json::to_vec(message).unwrap();
    assert!(u64::try_from(bytes.len()).unwrap() <= MAX_MESSAGE_BYTES);
    let temporary = path.with_extension("pending");
    std::fs::write(&temporary, bytes).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

fn read_message<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    assert!(std::fs::metadata(path).unwrap().len() <= MAX_MESSAGE_BYTES);
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a local MinIO bucket and LAMINAR_PROCESS_TEST_S3_ENDPOINT / _BUCKET"]
async fn independent_owners_restore_the_committed_cut_after_host_loss_and_rescale() {
    if let Some(root) = std::env::var_os(CHILD_ENV) {
        child::run(Path::new(&root)).await;
        return;
    }
    scenario::qualify(Runtime::Native).await;
    #[cfg(feature = "process-remote")]
    scenario::qualify(Runtime::RemoteRust).await;
}
