//! The cluster startup transaction: ordered phases from configuration
//! validation to a lease-fenced runtime, then hand-off to activation.
//!
//! Responsibility: run the visible startup protocol in order - validate
//! configuration, acquire the stable process identity, form discovery,
//! construct the database and lease-aware services, and bootstrap the catalog
//! under leader authority. Activation (readiness publication, assignment
//! certification, recovery, serving) continues in `activation`.
//!
//! Rollback eras:
//! - before discovery exists, `ProcessLeaseRuntime::drop` fences everything;
//! - once discovery started, every pre-controller failure stops it;
//! - once the database exists, failures additionally shut it down (an
//!   authority-loss failure revokes authority first);
//! - once the leader lease exists, failures run `cleanup_cluster_startup`,
//!   which races database shutdown against the process terminal token with a
//!   terminal bias.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::cluster::discovery::{
    GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo, NodeMetadata, NodeState,
    StaticDiscovery, StaticDiscoveryConfig,
};
use laminar_db::LaminarDB;

use super::activation::activate_cluster_serving;
use super::assignment::assignment_seed_participants;
use super::bootstrap::{construct_cluster_runtime, install_leader_lease_and_bootstrap_catalog};
use super::control_kv::OBJECT_STORE_CONTROL_IO_TIMEOUT;
use super::discovery::{stop_discovery_with_bound, DiscoveryImpl};
use super::leases::{acquire_process_lease, LeaderLeaseRuntime, ProcessLeaseRuntime};
use super::services::{build_control_store, install_cluster_tls, num_cpus, shuffle_advertise_addr};
use super::{numeric_node_id, ClusterHandle, ClusterStartupError, PROCESS_INCARNATION_TAG};
use crate::cluster_config::ClusterConfig;
use crate::config::ServerConfig;

pub(super) async fn cleanup_cluster_startup(
    discovery: &mut DiscoveryImpl,
    db: &LaminarDB,
    leader_lease: &mut LeaderLeaseRuntime,
    terminal: &tokio_util::sync::CancellationToken,
    force_terminal: bool,
) {
    leader_lease.cancel();
    let mut authority_lost = force_terminal || terminal.is_cancelled();
    let shutdown = db.shutdown();
    tokio::pin!(shutdown);
    let mut shutdown_result = None;
    if !authority_lost {
        tokio::select! {
            biased;
            () = terminal.cancelled() => authority_lost = true,
            result = &mut shutdown => shutdown_result = Some(result),
        }
    }

    if authority_lost {
        db.revoke_cluster_authority();
        let _ = stop_discovery_with_bound(discovery).await;
        leader_lease.stop().await;
    }

    if shutdown_result.is_none() {
        let _ = shutdown.await;
    }
    if !authority_lost {
        leader_lease.stop().await;
        let _ = stop_discovery_with_bound(discovery).await;
    }
}

/// Immutable, validated inputs every later phase relies on: node identity,
/// advertised addresses, key cardinality, and the validated event-time knobs.
pub(super) struct PreparedClusterBootstrap {
    pub(super) temporal_join_idle_history_retention: Option<Duration>,
    pub(super) source_idle_timeout: Option<Duration>,
    pub(super) event_time_max_future_skew: Duration,
    pub(super) node_id_str: String,
    pub(super) node_id: NodeId,
    pub(super) http_port: u16,
    pub(super) bind_host: String,
    pub(super) advertise_host: String,
    pub(super) key_groups: laminar_core::state::vnode::KeyGroupCount,
}

fn prepare_cluster_bootstrap(
    config: &ServerConfig,
    cluster_cfg: &ClusterConfig,
) -> Result<PreparedClusterBootstrap, ClusterStartupError> {
    let temporal_join_idle_history_retention = config
        .server
        .validated_temporal_join_idle_history_retention()
        .map_err(|error| ClusterStartupError::EngineConstruction(format!("server.{error}")))?;
    let source_idle_timeout = config
        .server
        .validated_source_idle_timeout()
        .map_err(|error| ClusterStartupError::EngineConstruction(format!("server.{error}")))?;
    let event_time_max_future_skew = config
        .server
        .validated_event_time_max_future_skew()
        .map_err(|error| ClusterStartupError::EngineConstruction(format!("server.{error}")))?;
    let node_id_str = cluster_cfg.node_id.as_str().to_string();
    let node_id_num = numeric_node_id(&node_id_str);
    let node_id = NodeId(node_id_num);

    let bind_addr = &config.server.bind;
    let http_port = if let Some(colon) = bind_addr.rfind(':') {
        bind_addr[colon + 1..].parse::<u16>().unwrap_or(8080)
    } else {
        8080
    };

    // Extract the host part from bind address, handling IPv6.
    // Examples: "127.0.0.1:8080" → "127.0.0.1", "[::1]:8080" → "[::1]"
    let bind_host = if let Some(bracket_end) = bind_addr.rfind(']') {
        // IPv6: "[::1]:8080" — take up to and including ']'
        &bind_addr[..=bracket_end]
    } else if let Some(colon) = bind_addr.rfind(':') {
        // IPv4: "127.0.0.1:8080" — take before the last ':'
        &bind_addr[..colon]
    } else {
        bind_addr.as_str()
    };

    let host_trimmed = bind_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host_trimmed == "0.0.0.0" || host_trimmed == "::" || host_trimmed.is_empty();
    let advertise_host = if let Some(ref host) = cluster_cfg.discovery.advertise_host {
        host.clone()
    } else if ip_wildcard {
        let hostname = gethostname::gethostname();
        let hostname = hostname.to_string_lossy().into_owned();
        if hostname.is_empty() {
            "127.0.0.1".to_string()
        } else {
            hostname
        }
    } else {
        bind_host.to_string()
    };

    if !matches!(cluster_cfg.discovery.strategy.as_str(), "gossip" | "static") {
        return Err(ClusterStartupError::Discovery(format!(
            "unsupported discovery strategy {:?}; expected \"gossip\" or \"static\"",
            cluster_cfg.discovery.strategy
        )));
    }
    if cluster_cfg.discovery.seeds.is_empty() {
        return Err(ClusterStartupError::Discovery(
            "cluster mode requires [discovery].seeds — list every node's \
             gossip address including this one (e.g. [\"node-0:7946\", \
             \"node-1:7946\"]); expected membership is derived from it"
                .into(),
        ));
    }
    let key_groups = config.server.resolved_key_groups();

    Ok(PreparedClusterBootstrap {
        temporal_join_idle_history_retention,
        source_idle_timeout,
        event_time_max_future_skew,
        node_id_str,
        node_id,
        http_port,
        bind_host: bind_host.to_string(),
        advertise_host,
        key_groups,
    })
}

/// The durable process identity this boot owns: control-plane store, leader
/// lease store, TLS selection, and the acquired process lease with its
/// renewal/terminal/fence tasks.
pub(super) struct AcquiredClusterIdentity {
    pub(super) control_store: Arc<dyn object_store::ObjectStore>,
    pub(super) lease_store: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    pub(super) lease_cfg: laminar_core::cluster::control::LeaderLeaseConfig,
    pub(super) process_incarnation: uuid::Uuid,
    pub(super) process_lease_ttl_ms: i64,
    pub(super) process_lease_authority:
        Arc<laminar_core::cluster::control::process_lease::ProcessLeaseAuthority>,
    pub(super) process_lease: ProcessLeaseRuntime,
}

async fn acquire_cluster_identity(
    config: &ServerConfig,
    cluster_cfg: &ClusterConfig,
    node_id: NodeId,
) -> Result<AcquiredClusterIdentity, ClusterStartupError> {
    // Claim the stable node identity before discovery can publish a duplicate member. The
    // durable recovery authority is deliberately not published until the database runtime exists.
    if CheckpointStorageScope::for_url(&config.checkpoint.url)
        != CheckpointStorageScope::ClusterShared
    {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster mode requires ClusterShared checkpoint storage".into(),
        ));
    }
    let control_store = build_control_store(config)?;
    let lease_cfg = laminar_core::cluster::control::LeaderLeaseConfig::default();
    let lease_ttl_ms = i64::try_from(lease_cfg.ttl.as_millis()).map_err(|_| {
        ClusterStartupError::EngineConstruction(
            "leader lease TTL exceeds the durable diagnostic range".into(),
        )
    })?;
    let lease_store = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
        Arc::clone(&control_store),
        lease_ttl_ms,
    ));
    lease_store
        .verify_store_contract(OBJECT_STORE_CONTROL_IO_TIMEOUT)
        .await
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "cluster control store does not provide required create/update fencing: {error}"
            ))
        })?;
    install_cluster_tls(&cluster_cfg.discovery)?;
    let process_incarnation = uuid::Uuid::new_v4();
    let process_lease_config = laminar_core::cluster::control::ProcessLeaseConfig::default();
    let process_lease_ttl_ms =
        i64::try_from(process_lease_config.ttl.as_millis()).map_err(|_| {
            ClusterStartupError::EngineConstruction(
                "process lease TTL exceeds i64 milliseconds".into(),
            )
        })?;
    let process_lease_authority = Arc::new(
        laminar_core::cluster::control::process_lease::ProcessLeaseAuthority::new(
            Arc::clone(&control_store),
            process_lease_config.ttl,
        )
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "construct shared process lease authority: {error}"
            ))
        })?,
    );
    let process_lease_store = process_lease_authority.store_for(node_id);
    let process_lease = acquire_process_lease(
        process_lease_store,
        process_incarnation,
        process_lease_config,
    )
    .await?;
    info!(
        term = process_lease.acquired.term,
        ttl_seconds = process_lease_config.ttl.as_secs(),
        "Stable node identity lease acquired"
    );

    Ok(AcquiredClusterIdentity {
        control_store,
        lease_store,
        lease_cfg,
        process_incarnation,
        process_lease_ttl_ms,
        process_lease_authority,
        process_lease,
    })
}

/// Discovery formed to the configured exact roster: the running discovery
/// implementation, the eligible peer view, the certified startup roster, the
/// bound shuffle receiver, and the local membership record.
pub(super) struct FormedCluster {
    pub(super) discovery: DiscoveryImpl,
    pub(super) peers: Vec<NodeInfo>,
    pub(super) startup_participants: Vec<laminar_core::checkpoint::CheckpointParticipant>,
    pub(super) shuffle_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    pub(super) shuffle_advertise: String,
    pub(super) local_node: NodeInfo,
}

async fn form_cluster_discovery(
    cluster_cfg: &ClusterConfig,
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
) -> Result<FormedCluster, ClusterStartupError> {
    let AcquiredClusterIdentity {
        process_incarnation,
        ..
    } = identity;
    let process_incarnation = *process_incarnation;
    let process_lease = &mut identity.process_lease;
    let PreparedClusterBootstrap {
        node_id, bind_host, ..
    } = prepared;
    let node_id = *node_id;
    let bind_host = bind_host.clone();
    let process_lease_terminal = process_lease.terminal_token();
    let (shuffle_receiver, shuffle_advertise, local_node) =
        bind_shuffle_and_local_node(cluster_cfg, prepared, process_incarnation, process_lease)
            .await?;

    let discovery = build_discovery(
        cluster_cfg,
        node_id,
        &bind_host,
        process_incarnation,
        process_lease.acquired.term,
        &local_node,
    )?;
    let mut discovery = discovery;
    let discovery_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = discovery.start() => Some(result),
    };
    match discovery_start {
        Some(Ok(())) => {}
        Some(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::Discovery(error.to_string()));
        }
        None => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while starting discovery".into(),
            ));
        }
    }
    info!("Discovery layer started");

    let peers = wait_for_cluster_formation(
        cluster_cfg,
        cluster_cfg.formation_timeout,
        &mut discovery,
        &process_lease_terminal,
    )
    .await?;
    let startup_participants = verify_startup_roster(
        cluster_cfg,
        node_id,
        identity,
        &peers,
        &mut discovery,
        &process_lease_terminal,
    )
    .await?;

    Ok(FormedCluster {
        discovery,
        peers,
        startup_participants,
        shuffle_receiver,
        shuffle_advertise,
        local_node,
    })
}

/// Bind the shuffle receiver on an ephemeral port, derive its advertised
/// address, and build this node's membership record with the shuffle and
/// process-incarnation tags.
async fn bind_shuffle_and_local_node(
    cluster_cfg: &ClusterConfig,
    prepared: &PreparedClusterBootstrap,
    process_incarnation: uuid::Uuid,
    process_lease: &ProcessLeaseRuntime,
) -> Result<
    (
        Arc<laminar_core::shuffle::ShuffleReceiver>,
        String,
        NodeInfo,
    ),
    ClusterStartupError,
> {
    let PreparedClusterBootstrap {
        node_id_str,
        node_id,
        http_port,
        bind_host,
        advertise_host,
        ..
    } = prepared;
    let bind_host = bind_host.clone();
    let advertise_host = advertise_host.clone();
    // Bind ShuffleReceiver first to discover port and publish it in metadata tags.
    let bind_addr: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid shuffle bind host: {e}"))
    })?;
    let shuffle_receiver =
        laminar_core::shuffle::ShuffleReceiver::bind(node_id.0, bind_addr, process_incarnation)
            .await
            .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?;
    shuffle_receiver
        .install_process_lease_deadline(Arc::clone(&process_lease.deadline))
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "install inbound shuffle process lease: {error}"
            ))
        })?;
    let shuffle_receiver = Arc::new(shuffle_receiver);
    let shuffle_advertise = shuffle_advertise_addr(shuffle_receiver.local_addr(), &advertise_host);

    let mut local_node = NodeInfo {
        id: *node_id,
        name: node_id_str.clone(),
        rpc_address: format!("{advertise_host}:{http_port}"),
        state: NodeState::Joining,
        metadata: NodeMetadata {
            cores: num_cpus(),
            memory_bytes: 0,
            failure_domain: cluster_cfg.discovery.failure_domain.clone(),
            tags: std::collections::HashMap::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        last_heartbeat_ms: 0,
    };
    local_node.metadata.tags.insert(
        laminar_core::shuffle::SHUFFLE_ADDR_KEY.to_string(),
        shuffle_advertise.clone(),
    );
    local_node.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.to_string(),
        process_incarnation.to_string(),
    );

    // 1. Start discovery layer
    info!(
        "Starting cluster discovery (strategy: {})",
        cluster_cfg.discovery.strategy
    );
    Ok((shuffle_receiver, shuffle_advertise, local_node))
}

/// Construct the configured discovery implementation. Both strategies carry
/// the process lease term as their process generation and this boot's
/// incarnation in the local membership record.
fn build_discovery(
    cluster_cfg: &ClusterConfig,
    node_id: NodeId,
    bind_host: &str,
    process_incarnation: uuid::Uuid,
    process_lease_term: u64,
    local_node: &NodeInfo,
) -> Result<DiscoveryImpl, ClusterStartupError> {
    let discovery: DiscoveryImpl = match cluster_cfg.discovery.strategy.as_str() {
        "gossip" => {
            let gossip_config = GossipDiscoveryConfig {
                gossip_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                seed_nodes: cluster_cfg.discovery.seeds.clone(),
                gossip_interval: std::time::Duration::from_secs(1),
                phi_threshold: 8.0,
                dead_node_grace_period: std::time::Duration::from_secs(60),
                cluster_id: "laminardb".to_string(),
                node_id,
                process_generation: process_lease_term,
                local_node: local_node.clone(),
                advertise_host: cluster_cfg.discovery.advertise_host.clone(),
            };
            DiscoveryImpl::Gossip(GossipDiscovery::new(gossip_config))
        }
        "static" => {
            let static_config = StaticDiscoveryConfig {
                local_node: local_node.clone(),
                seeds: cluster_cfg.discovery.seeds.clone(),
                heartbeat_interval: std::time::Duration::from_secs(1),
                suspect_threshold: 3,
                dead_threshold: 10,
                listen_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                process_generation: process_lease_term,
                process_incarnation,
            };
            DiscoveryImpl::Static(StaticDiscovery::new(static_config))
        }
        strategy => {
            return Err(ClusterStartupError::Discovery(format!(
                "unsupported discovery strategy {strategy:?}; expected \"gossip\" or \"static\""
            )));
        }
    };
    Ok(discovery)
}

/// Wait until discovery observes the configured exact peer count. Seeds
/// include self by convention, so the target is `seeds.len() - 1`. Discovery
/// is stopped on every failure path before the error escapes.
async fn wait_for_cluster_formation(
    cluster_cfg: &ClusterConfig,
    formation_timeout: Duration,
    discovery: &mut DiscoveryImpl,
    terminal: &tokio_util::sync::CancellationToken,
) -> Result<Vec<NodeInfo>, ClusterStartupError> {
    // 2. Wait for expected membership. Seeds include self by
    // convention (every node lists the full cluster), so the target
    // is `seeds.len() - 1`.
    let expected_peers = cluster_cfg.discovery.seeds.len().saturating_sub(1);
    let deadline = std::time::Instant::now() + formation_timeout;
    let mut last_seen = 0usize;
    let peers: Vec<NodeInfo> = loop {
        let discovered = tokio::select! {
            biased;
            () = terminal.cancelled() => None,
            result = discovery.peers() => Some(result),
        };
        let Some(discovered) = discovered else {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during cluster formation".into(),
            ));
        };
        if let Ok(discovered) = discovered {
            let eligible: Vec<NodeInfo> = discovered
                .into_iter()
                .filter(|peer| matches!(peer.state, NodeState::Joining | NodeState::Active))
                .collect();
            last_seen = eligible.len();
            if eligible.len() >= expected_peers {
                break eligible;
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::FormationTimeout {
                found: last_seen,
                needed: expected_peers,
            });
        }
        tokio::select! {
            biased;
            () = terminal.cancelled() => {
                let _ = discovery.stop().await;
                return Err(ClusterStartupError::AuthorityLost(
                    "stable node identity lease was lost during cluster formation".into(),
                ));
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    };
    info!(
        "Discovered {} peer(s) (expected {})",
        peers.len(),
        expected_peers
    );
    Ok(peers)
}

/// Verify the advertised startup roster against every peer's durable
/// stable-node lease, bounded by the shared-namespace proof timeout.
async fn verify_startup_roster(
    cluster_cfg: &ClusterConfig,
    node_id: NodeId,
    identity: &AcquiredClusterIdentity,
    peers: &[NodeInfo],
    discovery: &mut DiscoveryImpl,
    terminal: &tokio_util::sync::CancellationToken,
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    let AcquiredClusterIdentity {
        process_incarnation,
        process_lease_ttl_ms,
        control_store,
        ..
    } = identity;
    let process_incarnation = *process_incarnation;
    let process_lease_ttl_ms = *process_lease_ttl_ms;
    let control_store = Arc::clone(control_store);
    let formation_timeout = cluster_cfg.formation_timeout;
    let expected_seeds = cluster_cfg.discovery.seeds.len();
    let roster_timeout =
        formation_timeout.min(laminar_core::cluster::control::MAX_SHARED_NAMESPACE_PROOF_TIMEOUT);
    let roster = tokio::select! {
        biased;
        () = terminal.cancelled() => None,
        result = tokio::time::timeout(
            roster_timeout,
            assignment_seed_participants(
                laminar_core::state::NodeId(node_id.0),
                process_incarnation,
                peers,
                &control_store,
                process_lease_ttl_ms,
            ),
        ) => Some(result),
    };
    let Some(roster) = roster else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost during startup roster validation".into(),
        ));
    };
    let startup_participants = match roster {
        Ok(Ok(participants)) => participants,
        Ok(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
        Err(_) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "exact startup roster lease validation exceeded {roster_timeout:?}"
            )));
        }
    };
    let expected_participants = expected_seeds;
    if startup_participants.len() != expected_participants {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::Discovery(format!(
            "startup roster contains {} participants but the configured exact roster requires {expected_participants}",
            startup_participants.len()
        )));
    }
    Ok(startup_participants)
}

/// Start a LaminarDB server in cluster (multi-node) mode.
pub async fn start_cluster(
    config: ServerConfig,
    cluster_cfg: ClusterConfig,
    config_path: PathBuf,
) -> Result<ClusterHandle, ClusterStartupError> {
    let prepared = prepare_cluster_bootstrap(&config, &cluster_cfg)?;
    let mut identity = acquire_cluster_identity(&config, &cluster_cfg, prepared.node_id).await?;
    let mut formed = form_cluster_discovery(&cluster_cfg, &prepared, &mut identity).await?;
    let runtime =
        construct_cluster_runtime(&config, &cluster_cfg, &prepared, &mut identity, &mut formed)
            .await?;
    let gate = install_leader_lease_and_bootstrap_catalog(
        &config,
        &cluster_cfg,
        &mut identity,
        &runtime,
        &mut formed.discovery,
    )
    .await?;
    activate_cluster_serving(
        prepared.node_id_str,
        config,
        config_path,
        formed,
        identity,
        runtime,
        gate,
    )
    .await
}
