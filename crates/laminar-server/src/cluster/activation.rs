//! Post-Active cluster activation: readiness publication, assignment
//! certification, recovery, and the serving-gate commit.
//!
//! Responsibility: take the constructed, lease-fenced runtime through the
//! readiness commit protocol - verify the sealed catalog and leader-authority
//! budget, bind HTTP before local activation, announce Active, certify the
//! bootstrap assignment, start the pipeline, converge leader authority,
//! finish recovery, and open the serving gate into a `ClusterHandle`.
//!
//! INVARIANT: after the Active announcement may have committed remotely, every
//! failure synchronously fences local authority (leader lease, rebalance,
//! controller, startup fence, serving gate, database authority, process lease,
//! HTTP task) before the Left withdrawal and any cleanup I/O.

use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use laminar_core::cluster::discovery::{NodeInfo, NodeState};
use laminar_db::LaminarDB;

use super::assignment::{
    audit_stable_startup_assignment, startup_leader_authority_timeout,
    wait_for_startup_assignment_fence,
};
use super::bootstrap::{ConstructedClusterRuntime, LeaderGate};
use super::control_kv::OBJECT_STORE_CONTROL_IO_TIMEOUT;
use super::discovery::{
    announce_left_after_fence_with_bound, announce_node_state_with_bound, DiscoveryImpl,
};
use super::leases::{
    revoke_process_authority, LeaderLeaseRuntime, ProcessLeaseRuntime, PROCESS_LEASE_IO_TIMEOUT,
};
use super::services::start_cluster_http_api_before_activation;
use super::startup::{cleanup_cluster_startup, AcquiredClusterIdentity, FormedCluster};
use super::{
    abort_and_join_cluster_task, stop_bootstrap_rebalance_tasks, stop_rebalance_tasks,
    wait_for_cluster_task_exit, ClusterHandle, ClusterStartupError,
};
use crate::config::ServerConfig;

pub(super) fn fence_post_active_startup_failure(
    db: &LaminarDB,
    controller: &laminar_core::cluster::control::ClusterController,
    serving_gate: &crate::http::ServingGate,
    leader_lease_shutdown: &tokio_util::sync::CancellationToken,
    process_lease: &ProcessLeaseRuntime,
    rebalance_shutdown: Option<&tokio_util::sync::CancellationToken>,
    api_handle: &tokio::task::JoinHandle<()>,
) {
    leader_lease_shutdown.cancel();
    if let Some(shutdown) = rebalance_shutdown {
        shutdown.cancel();
    }
    controller.set_active(false);
    db.fence_cluster_startup();
    serving_gate.fence();
    db.revoke_cluster_authority();
    process_lease.fence_authority();
    api_handle.abort();
}

/// Rebalance task groups a post-Active failure must stop before rollback.
pub(super) enum PostActiveRebalance<'a> {
    /// Bootstrap certification tasks, stopped with the operation-timeout
    /// grace bound so they quiesce before pipeline initialization.
    Bootstrap {
        tasks: &'a mut Vec<tokio::task::JoinHandle<()>>,
        shutdown: &'a tokio_util::sync::CancellationToken,
        operation_timeout: std::time::Duration,
    },
    /// Runtime rebalance tasks, stopped with the standard shutdown bound.
    Runtime {
        tasks: &'a mut Vec<tokio::task::JoinHandle<()>>,
        shutdown: &'a tokio_util::sync::CancellationToken,
    },
    /// No rebalance tasks exist at this point of startup.
    None,
}

impl PostActiveRebalance<'_> {
    async fn stop(self) {
        match self {
            Self::Bootstrap {
                tasks,
                shutdown,
                operation_timeout,
            } => {
                let _ = stop_bootstrap_rebalance_tasks(tasks, shutdown, operation_timeout).await;
            }
            Self::Runtime { tasks, shutdown } => {
                let _ = stop_rebalance_tasks(tasks, shutdown).await;
            }
            Self::None => {}
        }
    }
}

/// The bootstrap rebalance control plane spawned for assignment
/// certification, with the grace bound its shutdown uses.
struct BootstrapRebalance {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    shutdown: tokio_util::sync::CancellationToken,
    operation_timeout: std::time::Duration,
}

/// Post-Active rollback owner. After the Active announcement may have
/// committed remotely, every failure must synchronously fence local authority
/// before the Left withdrawal and any cleanup I/O; `withdraw` then performs
/// the bounded asynchronous rollback in the fixed order: announce Left, stop
/// the rebalance group, abort and join HTTP, run the terminal-biased startup
/// cleanup.
pub(super) struct PostActiveFailure<'a> {
    discovery: &'a mut DiscoveryImpl,
    db: &'a LaminarDB,
    controller: &'a laminar_core::cluster::control::ClusterController,
    serving_gate: &'a crate::http::ServingGate,
    leader_lease: &'a mut LeaderLeaseRuntime,
    process_lease: &'a ProcessLeaseRuntime,
    api_handle: &'a mut tokio::task::JoinHandle<()>,
    terminal: &'a tokio_util::sync::CancellationToken,
    active: &'a NodeInfo,
}

impl<'a> PostActiveFailure<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        discovery: &'a mut DiscoveryImpl,
        db: &'a LaminarDB,
        controller: &'a laminar_core::cluster::control::ClusterController,
        serving_gate: &'a crate::http::ServingGate,
        leader_lease: &'a mut LeaderLeaseRuntime,
        process_lease: &'a ProcessLeaseRuntime,
        api_handle: &'a mut tokio::task::JoinHandle<()>,
        terminal: &'a tokio_util::sync::CancellationToken,
        active: &'a NodeInfo,
    ) -> Self {
        Self {
            discovery,
            db,
            controller,
            serving_gate,
            leader_lease,
            process_lease,
            api_handle,
            terminal,
            active,
        }
    }

    pub(super) fn fence_now(
        self,
        rebalance_shutdown: Option<&tokio_util::sync::CancellationToken>,
    ) -> Self {
        fence_post_active_startup_failure(
            self.db,
            self.controller,
            self.serving_gate,
            &self.leader_lease.shutdown,
            self.process_lease,
            rebalance_shutdown,
            self.api_handle,
        );
        self
    }

    pub(super) async fn withdraw(
        self,
        operation: &'static str,
        rebalance: PostActiveRebalance<'_>,
    ) {
        let _ = announce_left_after_fence_with_bound(self.discovery, self.active, operation).await;
        rebalance.stop().await;
        let _ = abort_and_join_cluster_task(self.api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            self.discovery,
            self.db,
            self.leader_lease,
            self.terminal,
            true,
        )
        .await;
    }
}

use super::serving::{open_cluster_serving, ServingLaunch};

/// Verify the sealed catalog and compute the leader-authority convergence
/// budget. Failures here are pre-Active: they run the startup cleanup without
/// a Left withdrawal except for an invalid authority budget, which withdraws
/// first because discovery is still advertising this node.
async fn verify_catalog_and_authority_budget(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
) -> Result<std::time::Duration, ClusterStartupError> {
    let ConstructedClusterRuntime {
        db, catalog_store, ..
    } = runtime;
    let LeaderGate {
        leader_lease,
        rebalance_config,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        local_node,
        ..
    } = formed;
    let discovery = &mut *discovery;
    let local_node = &*local_node;
    let catalog_verification = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = tokio::time::timeout(
            rebalance_config.checkpoint_timeout,
            catalog_store.load(),
        ) => Some(result),
    };
    let Some(catalog_verification) = catalog_verification else {
        cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true).await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while verifying the cluster catalog".into(),
        ));
    };
    let catalog_verification = match catalog_verification {
        Ok(result) => result,
        Err(_) => {
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, false)
                .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster catalog verification exceeded {:?}",
                rebalance_config.checkpoint_timeout
            )));
        }
    };
    match catalog_verification {
        Ok(Some(_)) => {}
        Ok(None) => {
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, false)
                .await;
            return Err(ClusterStartupError::EngineConstruction(
                "cluster catalog is not sealed before readiness announcement".into(),
            ));
        }
        Err(error) => {
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, false)
                .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "verify sealed cluster catalog before readiness announcement: {error}"
            )));
        }
    }
    if !process_lease.is_live() {
        cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true).await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before announcing cluster readiness".into(),
        ));
    }

    let Some(leader_authority_timeout) =
        startup_leader_authority_timeout(identity.lease_cfg, OBJECT_STORE_CONTROL_IO_TIMEOUT)
    else {
        if !process_lease_terminal.is_cancelled() {
            let mut left = local_node.clone();
            left.state = NodeState::Left;
            let _ = announce_node_state_with_bound(
                discovery,
                left,
                &process_lease_terminal,
                "withdraw node after invalid leader authority timeout",
            )
            .await;
        }
        cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, false).await;
        return Err(ClusterStartupError::EngineConstruction(
            "leader authority convergence timeout exceeds the monotonic timer range".into(),
        ));
    };
    Ok(leader_authority_timeout)
}

/// Bind the HTTP listener strictly before local activation. A pre-bind or
/// post-bind activation race aborts and joins the HTTP task and fails
/// startup.
async fn bind_http_and_announce_active(
    config: ServerConfig,
    config_path: PathBuf,
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
) -> Result<(Arc<crate::http::AppState>, tokio::task::JoinHandle<()>), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        snapshot_store,
        registry,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let registry = Arc::clone(registry);
    let LeaderGate {
        leader_lease,
        serving_gate,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let serving_gate = &*serving_gate;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        local_node,
        ..
    } = formed;
    let discovery = &mut *discovery;
    let local_node = &*local_node;
    // Bind and begin serving before this process can become an assignment owner. The startup
    // gate answers data/control requests with 503 until authority and recovery are established;
    // accepting now avoids holding requests in the kernel listen backlog for later replay.
    let cluster_components = crate::http::ClusterComponents {
        controller: Arc::clone(cluster_controller),
        snapshot_store: Arc::clone(snapshot_store),
        membership_rx: discovery.membership_watch(),
    };
    let http_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = start_cluster_http_api_before_activation(
            Arc::clone(db),
            registry,
            config_path.clone(),
            config,
            Arc::clone(serving_gate),
            cluster_components,
        ) => Some(result),
    };
    let Some(http_start) = http_start else {
        cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true).await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while binding the HTTP listener".into(),
        ));
    };
    let (app_state, mut api_handle) = match http_start {
        Ok(prepared) => prepared,
        Err(error) => {
            db.fence_cluster_startup();
            startup_controller.set_active(false);
            if !process_lease_terminal.is_cancelled() {
                let mut left = local_node.clone();
                left.state = NodeState::Left;
                let _ = announce_node_state_with_bound(
                    discovery,
                    left,
                    &process_lease_terminal,
                    "withdraw node after HTTP startup failure",
                )
                .await;
            }
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, false)
                .await;
            return Err(error);
        }
    };

    if !process_lease.is_live() {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        revoke_process_authority(
            db,
            serving_gate,
            &leader_lease.shutdown,
            &process_lease_terminal,
        );
        cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true).await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost while binding the HTTP listener".into(),
        ));
    }

    Ok((app_state, api_handle))
}

/// Take the constructed, lease-fenced runtime through the readiness commit
/// protocol and hand off to serving launch.
/// Announce Active readiness. The shuffle receiver and gated HTTP listener
/// are live, so only now may assignment eligibility be published - an
/// occupied API port can never leave an Active ghost node. A timeout or
/// transport error may follow a remotely committed Active announcement, so
/// every local authority is fenced synchronously before the Left withdrawal.
async fn announce_active_readiness(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    api_handle: &mut tokio::task::JoinHandle<()>,
) -> Result<laminar_core::cluster::discovery::NodeInfo, ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let LeaderGate {
        leader_lease,
        serving_gate,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let serving_gate = &*serving_gate;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster {
        discovery,
        local_node,
        ..
    } = formed;
    let discovery = &mut *discovery;
    let local_node = &*local_node;
    let mut active = local_node.clone();
    active.state = NodeState::Active;
    let active_announcement = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = tokio::time::timeout(
            PROCESS_LEASE_IO_TIMEOUT,
            discovery.announce(active.clone()),
        ) => Some(result),
    };
    let Some(active_announcement) = active_announcement else {
        let _ = abort_and_join_cluster_task(api_handle, "HTTP API server").await;
        revoke_process_authority(
            db,
            serving_gate,
            &leader_lease.shutdown,
            &process_lease_terminal,
        );
        cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true).await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while announcing cluster readiness".into(),
        ));
    };
    let active_announcement = match active_announcement {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(_) => Err(format!(
            "readiness announcement exceeded {PROCESS_LEASE_IO_TIMEOUT:?}"
        )),
    };
    if let Err(error) = active_announcement {
        // A timeout or transport error may follow a remotely committed Active announcement.
        // Fence every local authority synchronously before any rollback I/O.
        PostActiveFailure::new(
            discovery,
            db,
            startup_controller,
            serving_gate,
            leader_lease,
            process_lease,
            api_handle,
            &process_lease_terminal,
            &active,
        )
        .fence_now(None)
        .withdraw(
            "withdraw node after readiness announcement failure",
            PostActiveRebalance::None,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "announce cluster runtime readiness: {error}"
        )));
    }
    startup_controller.set_active(true);
    Ok(active)
}

/// Certify the bootstrap assignment: run the bootstrap rebalance control
/// plane until the exact local assignment fence is installed, stop and join
/// every bootstrap task before pipeline initialization, then audit that the
/// durable assignment still matches the certificate. Every failure is
/// post-Active.
async fn certify_bootstrap_assignment(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    api_handle: &mut tokio::task::JoinHandle<()>,
    active: &laminar_core::cluster::discovery::NodeInfo,
) -> Result<(), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        snapshot_store,
        vnode_registry,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let rebalance_config = gate.rebalance_config;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster { discovery, .. } = formed;
    let discovery = &mut *discovery;
    // Bootstrap assignment certification against the current process incarnation before building
    // the checkpoint coordinator. These tasks are stopped and joined after certification so no
    // watcher can advance the registry while startup snapshots its assignment.
    let bootstrap_rebalance_shutdown = tokio_util::sync::CancellationToken::new();
    let operation_timeout = rebalance_config.checkpoint_timeout;
    let mut bootstrap_rebalance_tasks = vec![
        laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(db),
            Arc::clone(snapshot_store),
            Arc::clone(vnode_registry),
            bootstrap_rebalance_shutdown.clone(),
            rebalance_config,
            Some(Arc::clone(startup_controller)),
        ),
        laminar_db::rebalance::spawn_rebalance_controller(
            Arc::clone(db),
            Arc::clone(startup_controller),
            Arc::clone(snapshot_store),
            Arc::clone(vnode_registry),
            bootstrap_rebalance_shutdown.clone(),
            rebalance_config,
        ),
    ];
    info!("Bootstrap assignment control plane started");

    let assignment_gate = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while certifying the startup assignment"
                    .into(),
            ))
        }
        () = wait_for_cluster_task_exit(api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while certifying the startup assignment".into(),
            ))
        }
        result = wait_for_startup_assignment_fence(
            startup_controller,
            vnode_registry,
            &bootstrap_rebalance_tasks,
        ) => result,
    };
    if let Err(error) = assignment_gate {
        PostActiveFailure::new(
            discovery,
            db,
            startup_controller,
            &gate.serving_gate,
            &mut gate.leader_lease,
            process_lease,
            api_handle,
            &process_lease_terminal,
            active,
        )
        .fence_now(Some(&bootstrap_rebalance_shutdown))
        .withdraw(
            "withdraw node after startup assignment failure",
            PostActiveRebalance::Bootstrap {
                tasks: &mut bootstrap_rebalance_tasks,
                shutdown: &bootstrap_rebalance_shutdown,
                operation_timeout: rebalance_config.checkpoint_timeout,
            },
        )
        .await;
        return Err(error);
    }
    let mut bootstrap = BootstrapRebalance {
        tasks: bootstrap_rebalance_tasks,
        shutdown: bootstrap_rebalance_shutdown,
        operation_timeout,
    };
    stop_bootstrap_after_certification(
        runtime,
        gate,
        identity,
        formed,
        api_handle,
        active,
        &mut bootstrap,
    )
    .await?;

    audit_certified_startup_assignment(
        runtime,
        gate,
        identity,
        formed,
        api_handle,
        active,
        rebalance_config.checkpoint_timeout,
    )
    .await?;

    Ok(())
}

/// Stop and join the bootstrap assignment tasks after certification. The
/// stop races the process terminal token and an HTTP task exit; either
/// preemption fences synchronously and withdraws before the join.
async fn stop_bootstrap_after_certification(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    api_handle: &mut tokio::task::JoinHandle<()>,
    active: &laminar_core::cluster::discovery::NodeInfo,
    bootstrap: &mut BootstrapRebalance,
) -> Result<(), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let LeaderGate {
        leader_lease,
        serving_gate,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let serving_gate = &*serving_gate;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster { discovery, .. } = formed;
    let discovery = &mut *discovery;
    let BootstrapRebalance {
        tasks,
        shutdown: bootstrap_rebalance_shutdown,
        operation_timeout: rebalance_config_checkpoint_timeout,
    } = bootstrap;
    let rebalance_config_checkpoint_timeout = *rebalance_config_checkpoint_timeout;
    let bootstrap_rebalance_tasks = tasks;
    let bootstrap_tasks_stop = stop_bootstrap_rebalance_tasks(
        bootstrap_rebalance_tasks,
        bootstrap_rebalance_shutdown,
        rebalance_config_checkpoint_timeout,
    );
    tokio::pin!(bootstrap_tasks_stop);
    let bootstrap_stop_result = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            fence_post_active_startup_failure(
                db,
                startup_controller,
                serving_gate,
                &leader_lease.shutdown,
                process_lease,
                Some(bootstrap_rebalance_shutdown),
                api_handle,
            );
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while stopping bootstrap assignment tasks"
                    .into(),
            ))
        }
        () = wait_for_cluster_task_exit(api_handle) => {
            fence_post_active_startup_failure(
                db,
                startup_controller,
                serving_gate,
                &leader_lease.shutdown,
                process_lease,
                Some(bootstrap_rebalance_shutdown),
                api_handle,
            );
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while stopping bootstrap assignment tasks".into(),
            ))
        }
        stopped = &mut bootstrap_tasks_stop => Ok(stopped),
    };
    let bootstrap_tasks_stopped = match bootstrap_stop_result {
        Ok(stopped) => stopped,
        Err(error) => {
            let _ = announce_left_after_fence_with_bound(
                discovery,
                active,
                "withdraw node after bootstrap assignment task interruption",
            )
            .await;
            let _ = bootstrap_tasks_stop.await;
            let _ = abort_and_join_cluster_task(api_handle, "HTTP API server").await;
            cleanup_cluster_startup(discovery, db, leader_lease, &process_lease_terminal, true)
                .await;
            return Err(error);
        }
    };
    if !bootstrap_tasks_stopped {
        PostActiveFailure::new(
            discovery,
            db,
            startup_controller,
            serving_gate,
            leader_lease,
            process_lease,
            api_handle,
            &process_lease_terminal,
            active,
        )
        .fence_now(Some(bootstrap_rebalance_shutdown))
        .withdraw(
            "withdraw node after bootstrap assignment task shutdown failure",
            PostActiveRebalance::None,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "bootstrap assignment tasks did not stop before pipeline initialization".into(),
        ));
    }
    Ok(())
}

/// Audit that the durable assignment still matches the certified startup
/// assignment while the HTTP task and process lease stay live.
async fn audit_certified_startup_assignment(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    api_handle: &mut tokio::task::JoinHandle<()>,
    active: &laminar_core::cluster::discovery::NodeInfo,
    checkpoint_timeout: std::time::Duration,
) -> Result<(), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        snapshot_store,
        vnode_registry,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let LeaderGate {
        leader_lease,
        serving_gate,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let serving_gate = &*serving_gate;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let FormedCluster { discovery, .. } = formed;
    let discovery = &mut *discovery;
    let stable_assignment = if !process_lease.is_live() {
        Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost before pipeline initialization".into(),
        ))
    } else {
        tokio::select! {
            biased;
            () = process_lease_terminal.cancelled() => {
                Err(ClusterStartupError::AuthorityLost(
                    "stable node identity lease was lost while auditing the startup assignment"
                        .into(),
                ))
            }
            () = wait_for_cluster_task_exit(api_handle) => {
                Err(ClusterStartupError::EngineConstruction(
                    "HTTP API server exited while auditing the startup assignment".into(),
                ))
            }
            result = audit_stable_startup_assignment(
                startup_controller,
                snapshot_store,
                vnode_registry,
                checkpoint_timeout,
            ) => result,
        }
    };
    if let Err(error) = stable_assignment {
        PostActiveFailure::new(
            discovery,
            db,
            startup_controller,
            serving_gate,
            leader_lease,
            process_lease,
            api_handle,
            &process_lease_terminal,
            active,
        )
        .fence_now(None)
        .withdraw(
            "withdraw node after bootstrap assignment changed during shutdown",
            PostActiveRebalance::None,
        )
        .await;
        return Err(error);
    }
    Ok(())
}

pub(super) async fn activate_cluster_serving(
    node_id_str: String,
    config: ServerConfig,
    config_path: PathBuf,
    mut formed: FormedCluster,
    identity: AcquiredClusterIdentity,
    runtime: ConstructedClusterRuntime,
    mut gate: LeaderGate,
) -> Result<ClusterHandle, ClusterStartupError> {
    let leader_authority_timeout =
        verify_catalog_and_authority_budget(&runtime, &mut gate, &identity, &mut formed).await?;

    let (app_state, mut api_handle) = bind_http_and_announce_active(
        config,
        config_path.clone(),
        &runtime,
        &mut gate,
        &identity,
        &mut formed,
    )
    .await?;

    let active =
        announce_active_readiness(&runtime, &mut gate, &identity, &mut formed, &mut api_handle)
            .await?;

    certify_bootstrap_assignment(
        &runtime,
        &mut gate,
        &identity,
        &mut formed,
        &mut api_handle,
        &active,
    )
    .await?;

    open_cluster_serving(
        config_path,
        formed,
        identity,
        runtime,
        gate,
        ServingLaunch {
            node_id_str,
            active,
            api_handle,
            app_state,
            leader_authority_timeout,
        },
    )
    .await
}
