//! Cluster runtime bootstrap: assignment resolution, shared-namespace
//! proof, database construction, and the lease-fenced local control plane.
//!
//! Responsibility: take a formed discovery and the acquired process identity
//! and produce the constructed runtime - the CAS-resolved vnode registry, the
//! installed controller, the lease-aware database, engine metrics, and the
//! supervised leader lease with its fenced serving gate and bootstrapped
//! catalog. Activation then publishes readiness against these resources.
//!
//! Ordering constraints:
//! - the process terminal token preempts every durable operation (biased
//!   selects) and the process lease is re-checked around shared-namespace
//!   proof and recovery-identity publication;
//! - pre-controller failures stop discovery; once the database exists they
//!   also shut it down (revoking authority on lease loss);
//! - once the leader lease exists, failures run `cleanup_cluster_startup`.

use std::sync::Arc;

use tracing::info;

use laminar_db::{LaminarDB, Profile};

use super::assignment::{
    resolve_vnode_assignment, wait_for_catalog_startup_authority, CatalogStartupAuthority,
};
use super::control_kv::ObjectStoreClusterKv;
use super::discovery::DiscoveryImpl;
use super::leases::{
    revoke_process_authority, LeaderLeaseRuntime, ProcessLeaseRuntime, PROCESS_LEASE_IO_TIMEOUT,
};
use super::services::{build_shuffle_sender, install_cluster_controller};
use super::startup::{
    cleanup_cluster_startup, AcquiredClusterIdentity, FormedCluster, PreparedClusterBootstrap,
};
use super::ClusterStartupError;
use crate::cluster_config::ClusterConfig;
use crate::config::ServerConfig;
use crate::server;

/// The constructed runtime: certified-assignment vnode registry, shared
/// snapshot store, installed controller, sealed catalog store, the database,
/// and the metrics registry HTTP will serve.
pub(super) struct ConstructedClusterRuntime {
    pub(super) vnode_registry: Arc<laminar_core::state::VnodeRegistry>,
    pub(super) snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    pub(super) cluster_controller: Arc<laminar_core::cluster::control::ClusterController>,
    pub(super) catalog_store: Arc<laminar_core::cluster::control::CatalogManifestStore>,
    pub(super) db: Arc<LaminarDB>,
    pub(super) registry: Arc<prometheus::Registry>,
}

/// The durably resolved boot assignment: CAS-resolved registry, the shared
/// snapshot store, the controller and recovery KV planes, and the verified
/// shared checkpoint namespaces.
struct ResolvedClusterAssignment {
    vnode_registry: Arc<laminar_core::state::VnodeRegistry>,
    snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    recovery_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    controller_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    verified_namespaces: laminar_core::cluster::control::VerifiedClusterNamespaces,
}

pub(super) async fn construct_cluster_runtime(
    config: &ServerConfig,
    cluster_cfg: &ClusterConfig,
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
) -> Result<ConstructedClusterRuntime, ClusterStartupError> {
    let resolved =
        resolve_assignment_and_verify_namespaces(cluster_cfg, prepared, identity, formed).await?;
    install_controller_and_build_database(config, prepared, identity, formed, resolved).await
}

/// Resolve the boot assignment and prove the shared checkpoint namespace:
/// CAS-resolve the initial vnode assignment, select the controller and
/// recovery KV planes, and verify every advertised participant writes the
/// same shared object-store namespaces.
async fn resolve_assignment_and_verify_namespaces(
    cluster_cfg: &ClusterConfig,
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
) -> Result<ResolvedClusterAssignment, ClusterStartupError> {
    let PreparedClusterBootstrap {
        node_id,
        key_groups,
        ..
    } = prepared;
    let node_id = *node_id;
    let key_groups = *key_groups;
    let AcquiredClusterIdentity { control_store, .. } = identity;
    let control_store = Arc::clone(control_store);
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        peers,
        startup_participants,
        ..
    } = formed;
    // Build the vnode registry. Exact same-formation genesis peers may preinstall v1; every
    // existing or racing different formation boots unassigned for the audited bootstrap path.
    let assignment = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = resolve_vnode_assignment(
            node_id,
            peers,
            u32::from(key_groups),
            Arc::clone(&control_store),
            startup_participants,
            process_lease.deadline.as_ref(),
        ) => Some(result),
    };
    let Some(assignment) = assignment else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while resolving the initial assignment".into(),
        ));
    };
    let (vnode_registry, snapshot_store) = match assignment {
        Ok(assignment) => assignment,
        Err(error) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
    };

    let (recovery_kv, controller_kv, verified_namespaces) =
        select_control_planes_and_prove_namespaces(cluster_cfg, prepared, identity, formed).await?;

    Ok(ResolvedClusterAssignment {
        vnode_registry,
        snapshot_store,
        recovery_kv,
        controller_kv,
        verified_namespaces,
    })
}

/// Install the cluster controller with its barrier server and build the
/// database with the full lease-aware service wiring.
async fn install_controller_and_build_database(
    config: &ServerConfig,
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    resolved: ResolvedClusterAssignment,
) -> Result<ConstructedClusterRuntime, ClusterStartupError> {
    let PreparedClusterBootstrap {
        temporal_join_idle_history_retention,
        source_idle_timeout,
        event_time_max_future_skew,
        ..
    } = prepared;
    let temporal_join_idle_history_retention = *temporal_join_idle_history_retention;
    let source_idle_timeout = *source_idle_timeout;
    let event_time_max_future_skew = *event_time_max_future_skew;
    let AcquiredClusterIdentity {
        control_store,
        lease_store,
        ..
    } = identity;
    let control_store = Arc::clone(control_store);
    let lease_store = Arc::clone(lease_store);
    // Build LaminarDB with Profile::Cluster
    let mut builder = LaminarDB::builder();
    builder = builder
        .profile(Profile::Cluster)
        .delivery_guarantee(config.server.delivery);
    if let Some(ref token) = config.server.console_token {
        builder = builder.http_auth_token(token.expose());
    }

    let cluster_controller =
        install_leased_controller(prepared, identity, formed, &resolved).await?;
    builder = builder.cluster_controller(Arc::clone(&cluster_controller));
    let ResolvedClusterAssignment {
        vnode_registry,
        snapshot_store,
        recovery_kv: _,
        controller_kv: _,
        verified_namespaces,
    } = resolved;
    // LaminarDB derives the participant namespace from the installed controller.
    builder = builder.incremental_emit(config.server.incremental_emit);
    if let Some(retention) = temporal_join_idle_history_retention {
        builder = builder.temporal_join_idle_history_retention(retention);
    }
    if let Some(timeout) = source_idle_timeout {
        builder = builder.source_idle_timeout(timeout);
    }
    builder = builder.event_time_max_future_skew(event_time_max_future_skew);
    builder = server::apply_verified_cluster_checkpoint_config(
        builder,
        &config.checkpoint,
        verified_namespaces,
    )
    .map_err(|error| {
        ClusterStartupError::EngineConstruction(format!("checkpoint storage: {error}"))
    })?;

    builder = builder.vnode_registry(Arc::clone(&vnode_registry));

    // Durable cluster 2PC decisions use a cluster-wide prefix in the shared checkpoint store.
    // Without this the leader's `Commit` announcement is the only commit
    // signal — ephemeral, so a mid-2PC leader crash produces split state.
    let decision_store = Arc::new(
        laminar_core::cluster::control::CheckpointDecisionStore::new(Arc::clone(&control_store)),
    );
    builder = builder.decision_store(decision_store);

    // Hand the builder the same snapshot store resolved in `resolve_vnode_assignment`
    // so the snapshot watcher and rebalance controller share one backing object.
    builder = builder.assignment_snapshot_store(Arc::clone(&snapshot_store));

    // Catalog sealing and leader fencing share one append-only CAS sequence.
    let catalog_store = Arc::new(laminar_core::cluster::control::CatalogManifestStore::new(
        Arc::clone(&lease_store),
    ));
    builder = builder.catalog_manifest_store(Arc::clone(&catalog_store));

    let db = build_leased_database(prepared, identity, formed, builder).await?;
    let startup_controller = &cluster_controller;
    let registry =
        publish_recovery_identity_and_metrics(config, identity, formed, &db, startup_controller)
            .await?;

    Ok(ConstructedClusterRuntime {
        vnode_registry,
        snapshot_store,
        cluster_controller,
        catalog_store,
        db,
        registry,
    })
}

/// Select the controller and recovery KV planes and prove every advertised
/// participant writes the same shared object-store namespaces.
async fn select_control_planes_and_prove_namespaces(
    cluster_cfg: &ClusterConfig,
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
) -> Result<
    (
        Arc<dyn laminar_core::cluster::control::ClusterKv>,
        Arc<dyn laminar_core::cluster::control::ClusterKv>,
        laminar_core::cluster::control::VerifiedClusterNamespaces,
    ),
    ClusterStartupError,
> {
    let PreparedClusterBootstrap { node_id, .. } = prepared;
    let node_id = *node_id;
    let AcquiredClusterIdentity {
        control_store,
        process_incarnation,
        process_lease_ttl_ms,
        ..
    } = identity;
    let control_store = Arc::clone(control_store);
    let process_incarnation = *process_incarnation;
    let process_lease_ttl_ms = *process_lease_ttl_ms;
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        startup_participants,
        ..
    } = formed;
    // Discovery/barrier traffic stays on the low-latency KV. Coordinated recovery always uses
    // the shared object store so generation, phases, and acknowledgements survive process loss.
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    let recovery_kv = Arc::new(ObjectStoreClusterKv::new(
        process_lease.acquired.clone(),
        Arc::clone(&process_lease.deadline),
        process_lease_ttl_ms,
        Arc::clone(&control_store),
        discovery.membership_watch(),
    )) as Arc<dyn ClusterKv>;
    let controller_kv = match &discovery {
        DiscoveryImpl::Gossip(gossip) => gossip
            .chitchat_handle()
            .map(|handle| Arc::new(ChitchatKv::from_handle(handle)) as Arc<dyn ClusterKv>),
        DiscoveryImpl::Static(_) => Some(Arc::clone(&recovery_kv)),
    };
    let Some(controller_kv) = controller_kv else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "gossip discovery started without its control channel".into(),
        ));
    };
    let namespace_control = Arc::clone(&controller_kv);
    let local_participant = laminar_core::checkpoint::CheckpointParticipant {
        node_id: node_id.0,
        boot_incarnation: process_incarnation,
    };
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before shared checkpoint namespace proof".into(),
        ));
    }
    let namespace_proof = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = laminar_core::cluster::control::prove_shared_object_store_namespaces(
            local_participant,
            startup_participants,
            namespace_control,
            Arc::clone(&control_store),
            cluster_cfg.formation_timeout,
        ) => Some(result),
    };
    let verified_namespaces = match namespace_proof {
        Some(Ok(namespaces)) => namespaces,
        Some(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(error.to_string()));
        }
        None => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during shared checkpoint namespace proof"
                    .into(),
            ));
        }
    };
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during shared checkpoint namespace proof".into(),
        ));
    }
    info!(
        participants = startup_participants.len(),
        "Shared checkpoint namespace verified"
    );
    Ok((recovery_kv, controller_kv, verified_namespaces))
}

/// Install the cluster controller and its barrier server under the process
/// lease, then register this node's locality and durable lease authority.
async fn install_leased_controller(
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    resolved: &ResolvedClusterAssignment,
) -> Result<Arc<laminar_core::cluster::control::ClusterController>, ClusterStartupError> {
    let PreparedClusterBootstrap {
        bind_host,
        advertise_host,
        ..
    } = prepared;
    let bind_host = bind_host.clone();
    let advertise_host = advertise_host.clone();
    let AcquiredClusterIdentity {
        process_lease_authority,
        ..
    } = identity;
    let process_lease_authority = Arc::clone(process_lease_authority);
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        local_node,
        ..
    } = formed;
    let ResolvedClusterAssignment {
        snapshot_store,
        recovery_kv,
        controller_kv,
        ..
    } = resolved;
    let controller = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = install_cluster_controller(
            Arc::clone(controller_kv),
            Arc::clone(recovery_kv),
            Arc::clone(snapshot_store),
            discovery.membership_watch(),
            &bind_host,
            &advertise_host,
            process_lease,
        ) => Some(result),
    };
    let Some(controller) = controller else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while installing cluster control".into(),
        ));
    };
    let cluster_controller = match controller {
        Ok(controller) => controller,
        Err(error) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
    };
    // Hand the controller this node's own locality so the topology-aware rebalancer can place
    // self correctly (peers' localities arrive via discovery; self is folded in by id only).
    cluster_controller.set_self_locality(laminar_core::state::Locality::parse(
        local_node.metadata.failure_domain.as_deref().unwrap_or(""),
    ));
    if let Err(error) =
        cluster_controller.set_process_lease_authority(Arc::clone(&process_lease_authority))
    {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error));
    }
    Ok(cluster_controller)
}

/// Wire the shuffle fabric into the builder and construct the database
/// runtime under the process lease.
async fn build_leased_database(
    prepared: &PreparedClusterBootstrap,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    mut builder: laminar_db::LaminarDbBuilder,
) -> Result<Arc<LaminarDB>, ClusterStartupError> {
    let PreparedClusterBootstrap { node_id, .. } = prepared;
    let node_id = *node_id;
    let AcquiredClusterIdentity {
        process_incarnation,
        ..
    } = identity;
    let process_incarnation = *process_incarnation;
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        shuffle_receiver,
        shuffle_advertise,
        ..
    } = formed;
    // Shuffle fabric. ShuffleReceiver was bound at startup.
    let shuffle_sender = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        sender = build_shuffle_sender(
            node_id.0,
            discovery,
            shuffle_advertise.clone(),
            discovery.membership_watch(),
            process_incarnation,
        ) => Some(sender),
    };
    let Some(shuffle_sender) = shuffle_sender else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while building the shuffle fabric".into(),
        ));
    };
    if let Err(error) =
        shuffle_sender.install_process_lease_deadline(Arc::clone(&process_lease.deadline))
    {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "install outbound shuffle process lease: {error}"
        )));
    }

    // Streaming aggregates go through the row-shuffle bridge driven by
    // `IncrementalAggState`; the DataFusion-native aggregate-rewrite path was removed.
    builder = builder
        .shuffle_sender(Arc::clone(&shuffle_sender))
        .shuffle_receiver(Arc::clone(shuffle_receiver))
        .target_partitions(1);

    let db = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = builder.build() => Some(result),
    };
    let Some(db) = db else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while constructing the database runtime".into(),
        ));
    };
    let db = match db {
        Ok(db) => db,
        Err(error) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(error.to_string()));
        }
    };
    if !process_lease.is_live() {
        db.revoke_cluster_authority();
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost while constructing the database runtime".into(),
        ));
    }
    Ok(db)
}

/// Publish this process's leased recovery incarnation and register engine
/// metrics on the database. Failures shut the database down and stop
/// discovery; authority loss additionally revokes cluster authority first.
async fn publish_recovery_identity_and_metrics(
    config: &ServerConfig,
    identity: &mut AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    db: &Arc<LaminarDB>,
    startup_controller: &Arc<laminar_core::cluster::control::ClusterController>,
) -> Result<Arc<prometheus::Registry>, ClusterStartupError> {
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster { discovery, .. } = formed;
    let recovery_identity = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = startup_controller.publish_leased_recovery_incarnation(&process_lease.acquired) => {
            Some(result)
        }
    };
    let Some(recovery_identity) = recovery_identity else {
        db.revoke_cluster_authority();
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost during recovery identity publication".into(),
        ));
    };
    if let Err(error) = recovery_identity {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "publish leased recovery process incarnation: {error}"
        )));
    }
    if !process_lease.is_live() {
        db.revoke_cluster_authority();
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during recovery identity publication".into(),
        ));
    }

    // Build prometheus registry before start() so connectors register on it.
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let pipeline_name = config
        .pipelines
        .first()
        .map_or("default", |p| p.name.as_str())
        .to_string();
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), hostname),
        ("pipeline".into(), pipeline_name),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    if let Err(error) = db.set_prometheus_registry(Arc::clone(&registry)) {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error.to_string()));
    }
    Ok(registry)
}
/// The lease-fenced local control plane: supervised leader lease runtime,
/// fenced serving gate, and the rebalance configuration certified startup
/// runs against.
pub(super) struct LeaderGate {
    pub(super) leader_lease: LeaderLeaseRuntime,
    pub(super) serving_gate: Arc<crate::http::ServingGate>,
    pub(super) rebalance_config: laminar_db::rebalance::RebalanceConfig,
}

pub(super) async fn install_leader_lease_and_bootstrap_catalog(
    config: &ServerConfig,
    cluster_cfg: &ClusterConfig,
    identity: &mut AcquiredClusterIdentity,
    runtime: &ConstructedClusterRuntime,
    discovery: &mut DiscoveryImpl,
) -> Result<LeaderGate, ClusterStartupError> {
    let ConstructedClusterRuntime { db, .. } = runtime;
    let (mut leader_lease, serving_gate) =
        install_lease_gate_and_catalog(config, cluster_cfg, identity, runtime, discovery).await?;
    let rebalance_config = fence_startup_and_prepare_recovery_generation(
        cluster_cfg,
        identity,
        db,
        discovery,
        &mut leader_lease,
    )
    .await?;

    Ok(LeaderGate {
        leader_lease,
        serving_gate,
        rebalance_config,
    })
}

/// Start the supervised leader lease and the fenced serving gate, install the
/// process-lease authority fence over the database and controller, and
/// bootstrap the cluster catalog under leader authority. Failures run the
/// terminal-biased startup cleanup.
async fn install_lease_gate_and_catalog(
    config: &ServerConfig,
    cluster_cfg: &ClusterConfig,
    identity: &mut AcquiredClusterIdentity,
    runtime: &ConstructedClusterRuntime,
    discovery: &mut DiscoveryImpl,
) -> Result<(LeaderLeaseRuntime, Arc<crate::http::ServingGate>), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        ..
    } = runtime;
    let AcquiredClusterIdentity {
        lease_store,
        lease_cfg,
        ..
    } = identity;
    let lease_store = Arc::clone(lease_store);
    let lease_cfg = *lease_cfg;
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let startup_controller = cluster_controller;
    // Fenced leader lease. Wiring the watch into the controller makes
    // `is_leader()` lease-aware, so every leader-gated path (checkpoint, 2PC,
    // rebalance, committer) inherits fencing: a stale candidate whose lease
    // expired stops being the leader. Renewal is gated on `is_gossip_leader` so
    // the lease owner converges to the gossip candidate. Wired before `start()`.
    cluster_controller.set_leader_lease_store(Arc::clone(&lease_store));
    let manager = match laminar_core::cluster::control::LeaderLeaseManager::new(
        Arc::clone(&lease_store),
        &process_lease.acquired,
        lease_cfg,
    ) {
        Ok(manager) => manager,
        Err(error) => {
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "leader lease manager: {error}"
            )));
        }
    };
    if let Err(error) = cluster_controller.set_leader_lease_runtime_watches(
        manager.lease_watch(),
        manager.owner().clone(),
        manager.deadline_watch(),
    ) {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error));
    }
    let token = tokio_util::sync::CancellationToken::new();
    let candidacy = cluster_controller.leader_candidacy_watch();
    let task = manager.spawn_supervised(token.clone(), candidacy, process_lease_terminal.clone());
    info!(
        "Leader lease manager started (ttl={}s)",
        lease_cfg.ttl.as_secs()
    );
    let mut leader_lease = LeaderLeaseRuntime::new(token, task);

    let serving_gate = Arc::new(crate::http::ServingGate::starting());
    if let Err(error) =
        serving_gate.install_process_lease_deadline(Arc::clone(&process_lease.deadline))
    {
        cleanup_cluster_startup(
            discovery,
            db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::AuthorityLost(error.into()));
    }
    process_lease.install_fence(
        Arc::clone(db),
        Arc::clone(startup_controller),
        Arc::clone(&serving_gate),
        leader_lease.shutdown_token(),
    );
    if !process_lease.is_live() {
        revoke_process_authority(
            db,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease_terminal,
        );
        cleanup_cluster_startup(
            discovery,
            db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before pipeline startup".into(),
        ));
    }

    bootstrap_cluster_catalog(
        config,
        cluster_cfg,
        runtime,
        discovery,
        process_lease,
        &mut leader_lease,
        &process_lease_terminal,
    )
    .await?;

    Ok((leader_lease, serving_gate))
}

/// Bootstrap the cluster catalog: wait for the durable leader lease or an
/// active peer, then replay the configuration DDL under that authority.
async fn bootstrap_cluster_catalog(
    config: &ServerConfig,
    cluster_cfg: &ClusterConfig,
    runtime: &ConstructedClusterRuntime,
    discovery: &mut DiscoveryImpl,
    process_lease: &ProcessLeaseRuntime,
    leader_lease: &mut LeaderLeaseRuntime,
    terminal: &tokio_util::sync::CancellationToken,
) -> Result<(), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let catalog_startup = tokio::select! {
        biased;
        () = terminal.cancelled() => {
            Err("stable node identity lease was lost during catalog bootstrap".to_string())
        }
        result = async {
            let authority = wait_for_catalog_startup_authority(
                startup_controller,
                cluster_cfg.formation_timeout,
            )
            .await?;
            server::execute_config_ddl(db, config, true)
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(authority)
        } => result,
    };
    match catalog_startup {
        Ok(CatalogStartupAuthority::DurableLease) => {
            info!("Cluster catalog sealed under the durable leader lease");
        }
        Ok(CatalogStartupAuthority::ActivePeer) => {
            info!("Cluster catalog replayed after observing an active peer");
        }
        Err(error) => {
            cleanup_cluster_startup(discovery, db, leader_lease, terminal, false).await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster catalog startup: {error}"
            )));
        }
    }
    if !process_lease.is_live() {
        cleanup_cluster_startup(discovery, db, leader_lease, terminal, true).await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during catalog bootstrap".into(),
        ));
    }
    Ok(())
}

/// Fence startup and prepare this process generation's recovery artifacts
/// before assignment certification.
async fn fence_startup_and_prepare_recovery_generation(
    cluster_cfg: &ClusterConfig,
    identity: &mut AcquiredClusterIdentity,
    db: &Arc<LaminarDB>,
    discovery: &mut DiscoveryImpl,
    leader_lease: &mut LeaderLeaseRuntime,
) -> Result<laminar_db::rebalance::RebalanceConfig, ClusterStartupError> {
    let process_lease = &mut identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    // Fence startup and audit this process generation's checkpoint artifacts before assignment
    // certification. The recovery monitor is installed only after the initial pipeline start.
    db.fence_cluster_startup();
    let recovery_generation = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = db.prepare_cluster_startup_recovery_generation(
            tokio::time::Instant::now() + PROCESS_LEASE_IO_TIMEOUT,
        ) => Some(result),
    };
    match recovery_generation {
        Some(Ok(_)) => {}
        Some(Err(error)) => {
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, false)
                .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster recovery generation bootstrap: {error}"
            )));
        }
        None => {
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true)
                .await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while restoring recovery generation".into(),
            ));
        }
    }
    let rebalance_config = laminar_db::rebalance::RebalanceConfig {
        placement_isolation_tier: cluster_cfg.discovery.placement_isolation_tier,
        ..laminar_db::rebalance::RebalanceConfig::default()
    };
    Ok(rebalance_config)
}
