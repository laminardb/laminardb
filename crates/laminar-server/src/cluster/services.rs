//! Cluster service wiring: control store, TLS, controller, barrier sync,
//! shuffle fabric, and the bind-before-activation HTTP listener.
//!
//! Responsibility: construct the durable control-plane object store, select the
//! process-wide cluster transport (mTLS or claimed plaintext), install the
//! `ClusterController` with its barrier sync server, bind the shuffle receiver
//! and sender, and start the HTTP listener strictly before this process can
//! become locally Active.
//!
//! Invariants:
//! - the HTTP listener binds before local activation; a pre-bind or post-bind
//!   race with activation aborts and joins the HTTP task and fails startup;
//! - a wildcard shuffle bind advertises the configured advertise host (or
//!   hostname) with the actually bound port;
//! - static discovery advertises shuffle addresses in heartbeat metadata and
//!   registers peers as they appear.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::watch;
use tracing::info;

use laminar_core::cluster::discovery::NodeInfo;
use laminar_db::LaminarDB;

use super::abort_and_join_cluster_task;
use super::control_kv::StaticClusterKv;
use super::discovery::DiscoveryImpl;
use super::leases::ProcessLeaseRuntime;
use crate::config::{DiscoverySection, ServerConfig};

use super::ClusterStartupError;
use crate::server;
pub(super) async fn start_cluster_http_api_before_activation(
    db: Arc<LaminarDB>,
    registry: Arc<prometheus::Registry>,
    config_path: PathBuf,
    config: ServerConfig,
    serving_gate: Arc<crate::http::ServingGate>,
    cluster: crate::http::ClusterComponents,
) -> Result<(Arc<crate::http::AppState>, tokio::task::JoinHandle<()>), ClusterStartupError> {
    let controller = Arc::clone(&cluster.controller);
    let local = controller.instance_id();
    if controller.live_instances().contains(&local) {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster HTTP listener must bind before local activation".into(),
        ));
    }

    let prepared = server::prepare_http_api(
        db,
        registry,
        config_path,
        config,
        serving_gate,
        Some(cluster),
    )
    .await
    .map_err(|error| ClusterStartupError::HttpStartup(error.to_string()))?;
    let (app_state, mut api_handle) = prepared
        .start()
        .await
        .map_err(|error| ClusterStartupError::HttpStartup(error.to_string()))?;
    if controller.live_instances().contains(&local) {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        return Err(ClusterStartupError::EngineConstruction(
            "local activation raced cluster HTTP listener startup".into(),
        ));
    }
    Ok((app_state, api_handle))
}

pub(super) fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Build the shared, cluster-wide control-plane object store (assignment snapshot plus
/// `ObjectStoreClusterKv`) from the cluster-shared checkpoint namespace.
pub(super) fn build_control_store(
    config: &ServerConfig,
) -> Result<Arc<dyn object_store::ObjectStore>, ClusterStartupError> {
    laminar_core::checkpoint::object_store_builder::build_object_store(
        &config.checkpoint.url,
        &config.checkpoint.storage,
    )
    .map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("control-plane object store: {e}"))
    })
}

/// Build a `ClusterController` from a ready KV handle and start its barrier sync
/// server. Shared by the gossip and static discovery paths.
pub(super) async fn install_cluster_controller(
    kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    recovery_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
    bind_host: &str,
    advertise_host: &str,
    process_lease: &ProcessLeaseRuntime,
) -> Result<Arc<laminar_core::cluster::control::ClusterController>, ClusterStartupError> {
    use laminar_core::cluster::control::ClusterController;

    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        process_lease.acquired.node,
        kv,
        recovery_kv,
        Some(snapshot_store),
        members_rx,
        process_lease.acquired.owner,
    ));
    controller
        .set_process_lease_deadline(Arc::clone(&process_lease.deadline))
        .map_err(ClusterStartupError::EngineConstruction)?;
    controller.set_active(false);
    controller.install_local_leader_proof_provider();

    let bind: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid barrier sync bind host: {e}"))
    })?;
    let bound = controller
        .start_leased_barrier_server(
            bind,
            Some(advertise_host.to_string()),
            &process_lease.acquired,
        )
        .await
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("cluster control endpoint start: {e}"))
        })?;
    info!("Barrier sync gRPC server listening on {bound}");
    info!(
        "ClusterController installed (leader={})",
        controller.is_leader()
    );
    Ok(controller)
}

/// Resolve the process-wide control-plane transport before any server/client
/// binds. Install mTLS when configured; otherwise atomically claim plaintext.
pub(super) fn install_cluster_tls(d: &DiscoverySection) -> Result<(), ClusterStartupError> {
    let configured = (
        &d.cluster_tls_cert,
        &d.cluster_tls_key,
        &d.cluster_tls_client_ca,
        &d.cluster_tls_server_name,
    );
    let (cert, key, ca, name) = match configured {
        (Some(cert), Some(key), Some(ca), Some(name)) => (cert, key, ca, name),
        (None, None, None, None) => {
            laminar_core::cluster::control::claim_cluster_plaintext().map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "select plaintext cluster transport before startup: {error}"
                ))
            })?;
            return Ok(());
        }
        _ => {
            return Err(ClusterStartupError::EngineConstruction(
                "cluster control-plane TLS requires cert, key, client CA, and server name".into(),
            ));
        }
    };
    let read = |p: &std::path::Path| {
        std::fs::read(p).map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("read {}: {e}", p.display()))
        })
    };
    let tls = laminar_core::cluster::control::ClusterTls::from_pem(
        &read(cert)?,
        &read(key)?,
        &read(ca)?,
        name,
    );
    laminar_core::cluster::control::set_cluster_tls(tls).map_err(|error| {
        ClusterStartupError::EngineConstruction(format!(
            "install cluster control-plane TLS before transport startup: {error}"
        ))
    })?;
    info!("cluster control-plane mTLS enabled (server_name={name})");
    Ok(())
}

/// Build an outbound shuffle sender. When gossip discovery is active,
/// publish `advertise_addr` under `SHUFFLE_ADDR_KEY` so peers find us, and
/// give the sender a KV handle for reverse lookup. Static discovery
/// uses `StaticClusterKv` to query peer shuffle addresses from TCP heartbeat metadata.
pub(super) async fn build_shuffle_sender(
    node_id: u64,
    discovery: &DiscoveryImpl,
    advertise_addr: String,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    process_incarnation: uuid::Uuid,
) -> Arc<laminar_core::shuffle::ShuffleSender> {
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    use laminar_core::shuffle::{ShuffleSender, SHUFFLE_ADDR_KEY};

    let sender = match discovery {
        DiscoveryImpl::Gossip(gossip) => {
            if let Some(handle) = gossip.chitchat_handle() {
                let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
                kv.write(SHUFFLE_ADDR_KEY, advertise_addr).await;
                ShuffleSender::with_kv(node_id, kv, process_incarnation)
            } else {
                ShuffleSender::new(node_id, process_incarnation)
            }
        }
        DiscoveryImpl::Static(_) => {
            let kv: Arc<dyn ClusterKv> = Arc::new(StaticClusterKv::new(membership_rx.clone()));
            ShuffleSender::with_kv(node_id, kv, process_incarnation)
        }
    };
    let sender = Arc::new(sender);

    // Static advertises shuffle addrs in heartbeat metadata; register peers as they appear.
    if matches!(discovery, DiscoveryImpl::Static(_)) {
        let sender_clone = Arc::clone(&sender);
        let mut rx = membership_rx;
        tokio::spawn(async move {
            loop {
                let members = rx.borrow().clone();
                for node in members {
                    if node.id.0 != node_id {
                        if let Some(addr) = node
                            .metadata
                            .tags
                            .get(SHUFFLE_ADDR_KEY)
                            .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
                        {
                            sender_clone.register_peer(node.id.0, addr);
                        }
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    sender
}

/// Compute the address peers should use to reach our `ShuffleReceiver`.
///
/// The receiver binds to `0.0.0.0:0` (any interface, ephemeral port), so
/// `local_addr.ip()` is the wildcard — publishing it unchanged leaves remote
/// senders unable to connect. Use the configured advertise host (matching the
/// HTTP/barrier endpoints for NAT/container deployments), falling back to
/// `gethostname` when it is itself a wildcard, keeping the actual bound port.
pub(super) fn shuffle_advertise_addr(
    local_addr: std::net::SocketAddr,
    advertise_host: &str,
) -> String {
    let port = local_addr.port();
    let host = advertise_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host == "0.0.0.0" || host == "::" || host.is_empty();
    if !ip_wildcard {
        return format!("{advertise_host}:{port}");
    }
    let hostname = gethostname::gethostname();
    let hostname = hostname.to_string_lossy();
    if hostname.is_empty() {
        local_addr.to_string()
    } else {
        format!("{hostname}:{port}")
    }
}
