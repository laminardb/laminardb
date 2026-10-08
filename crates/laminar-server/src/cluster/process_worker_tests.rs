//! Sealed server rejection of native Kafka ordering and cleanup of the bound Python child.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use super::*;
use crate::cluster_config::ClusterConfig;
use crate::config::ServerConfig;

fn config() -> ServerConfig {
    let package = PathBuf::from(std::env::var_os("LAMINAR_PROCESS_REPLAY_PACKAGE").unwrap());
    let endpoint = std::env::var("LAMINAR_PROCESS_TEST_S3_ENDPOINT").unwrap();
    let endpoint_address: std::net::SocketAddr =
        endpoint.strip_prefix("http://").unwrap().parse().unwrap();
    assert!(endpoint_address.ip().is_loopback() && endpoint_address.port() != 0);
    let bucket = std::env::var("LAMINAR_PROCESS_TEST_S3_BUCKET").unwrap();
    let gossip_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config: ServerConfig = toml::from_str(&format!(
        r#"
node_id = "python-worker-admission"
[server]
mode = "cluster"
bind = "127.0.0.1:0"
delivery = "at_least_once"
key_groups = 4
[checkpoint]
url = "s3://{bucket}/python-server-{}"
interval = "1h"
timeout = "30s"
[discovery]
strategy = "static"
seeds = ["127.0.0.1:{gossip_port}"]
gossip_port = {gossip_port}
[[process_function]]
source = "events"
output = "activity"
source_sql = """
CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL,
ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)
FROM KAFKA ('bootstrap.servers' = '127.0.0.1:1', 'group.id' = 'python-server',
'topic' = 'events', 'startup.mode' = 'earliest') FORMAT JSON
"""
manifest = "{}/manifest.json"
handler_file = "{}/handler/replay_handler.py"
function = "handle"
python = "{}/runtime/bin/python3.13"
runtime_root = "{}/runtime"
python_paths = ["{}/runtime/lib/python3.13/site-packages"]
timeout = "10s"
"#,
        uuid::Uuid::new_v4(),
        package.display(),
        package.display(),
        package.display(),
        package.display(),
        package.display(),
    ))
    .unwrap();
    config.checkpoint.storage = std::collections::HashMap::from([
        ("aws_endpoint".into(), endpoint),
        ("aws_allow_http".into(), "true".into()),
        ("aws_region".into(), "us-east-1".into()),
        ("aws_access_key_id".into(), "minioadmin".into()),
        ("aws_secret_access_key".into(), "minioadmin".into()),
    ]);
    crate::config::validate_process_functions(&config).unwrap();
    config
}

fn worker_pids(manifest: &std::path::Path) -> BTreeSet<u32> {
    let mut matches = BTreeSet::new();
    for task in std::fs::read_dir("/proc/self/task").unwrap() {
        let children = std::fs::read_to_string(task.unwrap().path().join("children")).unwrap();
        for child in children.split_whitespace() {
            let pid: u32 = child.parse().unwrap();
            let Ok(command) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
                continue;
            };
            if command
                .split(|byte| *byte == 0)
                .any(|arg| arg == manifest.as_os_str().as_encoded_bytes())
            {
                matches.insert(pid);
            }
        }
    }
    matches
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a sealed read-only Linux package and loopback MinIO"]
async fn cluster_python_rejects_unordered_kafka_and_reaps_worker() {
    let config = config();
    let cluster = ClusterConfig::from_server_config(&config).unwrap().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("server.toml");
    std::fs::write(&path, "# qualification config anchor\n").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(90),
        start_cluster(config.clone(), cluster, path),
    )
    .await
    .expect("sealed server rejection exceeded its bound");
    let Err(error) = result else {
        panic!("native Kafka order must not admit guaranteed process replay");
    };
    assert!(
        error.to_string().contains("single-channel replay-order"),
        "{error}"
    );
    assert!(worker_pids(&config.process_functions[0].manifest).is_empty());
}
