//! Serving launch: pipeline start, coordinated recovery, leader-authority
//! convergence, and the serving-gate commit into a `ClusterHandle`.
//!
//! Responsibility: after the bootstrap assignment is certified, start the
//! pipeline under the exact assignment fence, install the coordinated
//! recovery monitor, start the runtime rebalance control plane, converge
//! durable leader authority with the certified assignment, release intake
//! from coordinated recovery, and finally open the serving gate - the last
//! serving-authority commit - before handing every owned resource to the
//! `ClusterHandle`.
//!
//! INVARIANT: every failure here is post-Active; `PostActiveFailure` fences
//! local authority synchronously before the Left withdrawal and cleanup I/O.

use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use laminar_db::ClusterStartupDisposition;

use super::activation::{PostActiveFailure, PostActiveRebalance};
use super::assignment::wait_for_startup_leader_authority;
use super::bootstrap::{ConstructedClusterRuntime, LeaderGate};
use super::discovery::spawn_membership_watcher;
use super::startup::{AcquiredClusterIdentity, FormedCluster};
use super::{
    wait_for_cluster_task_exit, wait_for_rebalance_task_exit, ClusterHandle, ClusterStartupError,
    STARTUP_RECOVERY_TIMEOUT,
};
use crate::server;

/// The post-Active readiness outputs the serving launch commits: the Active
/// membership record, the bound HTTP API task and app state, and the
/// leader-authority convergence budget.
/// Start the certified pipeline and the coordinated recovery monitor, then
/// launch the runtime rebalance control plane.
async fn start_pipeline_and_recovery_monitor(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    launch: &mut ServingLaunch,
) -> Result<
    (
        Vec<tokio::task::JoinHandle<()>>,
        tokio_util::sync::CancellationToken,
    ),
    ClusterStartupError,
> {
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
        rebalance_config,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let serving_gate = &*serving_gate;
    let rebalance_config = *rebalance_config;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let active = &launch.active;
    let api_handle = &mut launch.api_handle;
    let FormedCluster { discovery, .. } = formed;
    let discovery = &mut *discovery;

    let pipeline_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while starting the pipeline".into(),
            ))
        }
        () = wait_for_cluster_task_exit(api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while starting the pipeline".into(),
            ))
        }
        result = db.start() => result.map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("pipeline start: {error}"))
        }),
    };
    if let Err(error) = pipeline_start {
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
            "withdraw node after pipeline startup failure",
            PostActiveRebalance::None,
        )
        .await;
        return Err(error);
    }
    info!("Pipeline started from the certified assignment");

    if let Err(error) = db.enable_coordinated_recovery() {
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
            "withdraw node after recovery monitor initialization failure",
            PostActiveRebalance::None,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "cluster recovery monitor initialization: {error}"
        )));
    }

    // Runtime launch is complete. Start the fresh long-lived watcher/rebalancer immediately: a
    // second process failure during startup recovery must still be able to authorize and follow a
    // successor assignment. Recovery fencing keeps source/checkpoint work closed meanwhile.
    let rebalance_shutdown = tokio_util::sync::CancellationToken::new();
    let rebalance_tasks = vec![
        laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(db),
            Arc::clone(snapshot_store),
            Arc::clone(vnode_registry),
            rebalance_shutdown.clone(),
            rebalance_config,
            Some(Arc::clone(startup_controller)),
        ),
        laminar_db::rebalance::spawn_rebalance_controller(
            Arc::clone(db),
            Arc::clone(startup_controller),
            Arc::clone(snapshot_store),
            Arc::clone(vnode_registry),
            rebalance_shutdown.clone(),
            rebalance_config,
        ),
    ];
    info!("Rebalance control plane started");
    Ok((rebalance_tasks, rebalance_shutdown))
}

/// Converge durable leader authority with the certified assignment, finish
/// cluster startup, and release intake from coordinated recovery.
async fn certify_startup_authority_and_recover(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    launch: &mut ServingLaunch,
    rebalance: &mut RuntimeRebalance,
) -> Result<(), ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        vnode_registry,
        ..
    } = runtime;
    let startup_controller = cluster_controller;
    let LeaderGate {
        leader_lease,
        serving_gate,
        rebalance_config,
        ..
    } = gate;
    let leader_lease = &mut *leader_lease;
    let serving_gate = &*serving_gate;
    let rebalance_config = *rebalance_config;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    let active = &launch.active;
    let api_handle = &mut launch.api_handle;
    let leader_authority_timeout = launch.leader_authority_timeout;
    let FormedCluster { discovery, .. } = formed;
    let discovery = &mut *discovery;
    let RuntimeRebalance {
        tasks: rebalance_tasks,
        shutdown: rebalance_shutdown,
    } = rebalance;

    let startup_gate = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while certifying startup authority".into(),
            ))
        }
        () = wait_for_rebalance_task_exit(rebalance_tasks) => {
            Err(ClusterStartupError::EngineConstruction(
                "rebalance control task exited while certifying startup authority".into(),
            ))
        }
        () = wait_for_cluster_task_exit(api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while certifying startup authority".into(),
            ))
        }
        result = async {
            wait_for_startup_leader_authority(
                startup_controller,
                vnode_registry,
                leader_authority_timeout,
            )
            .await?;
            info!(
                timeout = ?leader_authority_timeout,
                "Durable leader authority converged with the certified assignment"
            );
            let authority_deadline =
                tokio::time::Instant::now() + rebalance_config.checkpoint_timeout;
            db.finish_cluster_startup(authority_deadline)
                .await
                .map_err(|error| {
                    ClusterStartupError::EngineConstruction(format!(
                        "cluster startup recovery fence: {error}"
                    ))
                })
        } => result,
    };
    let startup_disposition = match startup_gate {
        Ok(disposition) => disposition,
        Err(error) => {
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
            .fence_now(Some(rebalance_shutdown))
            .withdraw(
                "withdraw node after startup authority failure",
                PostActiveRebalance::Runtime {
                    tasks: rebalance_tasks,
                    shutdown: rebalance_shutdown,
                },
            )
            .await;
            return Err(error);
        }
    };
    match startup_disposition {
        ClusterStartupDisposition::Serving => {
            info!("Cluster assignment certified; source intake opened");
        }
        ClusterStartupDisposition::Idle => {
            info!("Cluster worker owns no vnodes; data plane remains fenced pending assignment");
        }
        ClusterStartupDisposition::RecoveryFenced => {
            wait_for_coordinated_recovery_release(
                runtime, gate, identity, formed, launch, rebalance,
            )
            .await?;
        }
    }
    Ok(())
}

/// The runtime rebalance control plane started by the serving launch.
struct RuntimeRebalance {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    shutdown: tokio_util::sync::CancellationToken,
}

pub(super) struct ServingLaunch {
    pub(super) node_id_str: String,
    pub(super) active: laminar_core::cluster::discovery::NodeInfo,
    pub(super) api_handle: tokio::task::JoinHandle<()>,
    pub(super) app_state: Arc<crate::http::AppState>,
    pub(super) leader_authority_timeout: std::time::Duration,
}

/// Wait for coordinated startup recovery to release source intake, bounded by
/// the startup recovery timeout and preempted by lease, rebalance, and HTTP
/// Wait for coordinated startup recovery to release source intake, bounded by
/// the startup recovery timeout and preempted by lease, rebalance, and HTTP
/// task exits.
async fn wait_for_coordinated_recovery_release(
    runtime: &ConstructedClusterRuntime,
    gate: &mut LeaderGate,
    identity: &AcquiredClusterIdentity,
    formed: &mut FormedCluster,
    launch: &mut ServingLaunch,
    rebalance: &mut RuntimeRebalance,
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
    let active = &launch.active;
    let api_handle = &mut launch.api_handle;
    let FormedCluster { discovery, .. } = formed;
    let discovery = &mut *discovery;
    let RuntimeRebalance {
        tasks: rebalance_tasks,
        shutdown: rebalance_shutdown,
    } = rebalance;
    info!("Cluster source intake remains fenced for coordinated recovery");
    let recovery_wait = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during coordinated startup recovery".into(),
            ))
        }
        () = wait_for_rebalance_task_exit(rebalance_tasks) => {
            Err(ClusterStartupError::EngineConstruction(
                "rebalance control task exited during coordinated startup recovery".into(),
            ))
        }
        () = wait_for_cluster_task_exit(api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited during coordinated startup recovery".into(),
            ))
        }
        result = tokio::time::timeout(STARTUP_RECOVERY_TIMEOUT, async {
            while db.cluster_intake_fenced() || startup_controller.is_recovering() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }) => result.map_err(|_| {
            ClusterStartupError::EngineConstruction(format!(
                "coordinated startup recovery did not release intake within {STARTUP_RECOVERY_TIMEOUT:?}"
            ))
        }),
    };
    if let Err(error) = recovery_wait {
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
        .fence_now(Some(rebalance_shutdown))
        .withdraw(
            "withdraw node after startup recovery failure",
            PostActiveRebalance::Runtime {
                tasks: rebalance_tasks,
                shutdown: rebalance_shutdown,
            },
        )
        .await;
        return Err(error);
    }
    Ok(())
}

pub(super) async fn open_cluster_serving(
    config_path: PathBuf,
    mut formed: FormedCluster,
    identity: AcquiredClusterIdentity,
    runtime: ConstructedClusterRuntime,
    mut gate: LeaderGate,
    mut launch: ServingLaunch,
) -> Result<ClusterHandle, ClusterStartupError> {
    let ConstructedClusterRuntime {
        db,
        cluster_controller,
        vnode_registry,
        ..
    } = &runtime;
    let startup_controller = cluster_controller;
    let process_lease = &identity.process_lease;
    let process_lease_terminal = process_lease.terminal_token();
    info!(
        assignment_version = vnode_registry.assignment_version(),
        "Bootstrap assignment certified; starting checkpoint recovery"
    );

    let (rebalance_tasks, rebalance_shutdown) = start_pipeline_and_recovery_monitor(
        &runtime,
        &mut gate,
        &identity,
        &mut formed,
        &mut launch,
    )
    .await?;
    let mut rebalance = RuntimeRebalance {
        tasks: rebalance_tasks,
        shutdown: rebalance_shutdown,
    };

    certify_startup_authority_and_recover(
        &runtime,
        &mut gate,
        &identity,
        &mut formed,
        &mut launch,
        &mut rebalance,
    )
    .await?;

    let ServingLaunch {
        node_id_str,
        active,
        mut api_handle,
        app_state,
        leader_authority_timeout: _,
    } = launch;
    let FormedCluster { mut discovery, .. } = formed;

    // An idle worker serves control-plane readiness while its data plane remains fenced until the
    // watcher grants ownership.
    let api_exited_before_serving = api_handle.is_finished();
    let process_lease_lost_before_serving = !process_lease.is_live();
    if api_exited_before_serving
        || process_lease_lost_before_serving
        || !app_state.open_startup_gate()
    {
        PostActiveFailure::new(
            &mut discovery,
            db,
            startup_controller,
            &gate.serving_gate,
            &mut gate.leader_lease,
            process_lease,
            &mut api_handle,
            &process_lease_terminal,
            &active,
        )
        .fence_now(Some(&rebalance.shutdown))
        .withdraw(
            "withdraw node after serving gate failure",
            PostActiveRebalance::Runtime {
                tasks: &mut rebalance.tasks,
                shutdown: &rebalance.shutdown,
            },
        )
        .await;
        let reason = if api_exited_before_serving {
            "HTTP API server exited before serving authority opened"
        } else if process_lease_lost_before_serving {
            "stable node identity lease was lost before HTTP serving authority opened"
        } else {
            "HTTP serving gate could not be opened"
        };
        return Err(ClusterStartupError::EngineConstruction(reason.into()));
    }
    let watcher_handle = server::spawn_config_watcher(&app_state, config_path);
    let membership_rx = discovery.membership_watch();
    let membership_handle = spawn_membership_watcher(&node_id_str, membership_rx);
    info!("Membership watcher started");

    info!("Cluster node '{node_id_str}' started");

    Ok(ClusterHandle {
        db: db.clone(),
        db_shutdown_complete: false,
        discovery,
        serving_gate: gate.serving_gate,
        api_handle,
        watcher_handle,
        membership_handle,
        local_node: active,
        cluster_controller: cluster_controller.clone(),
        snapshot_store: runtime.snapshot_store.clone(),
        vnode_count: vnode_registry.vnode_count(),
        leader_lease: gate.leader_lease,
        process_lease: identity.process_lease,
        rebalance_tasks: rebalance.tasks,
        rebalance_shutdown: rebalance.shutdown,
    })
}
