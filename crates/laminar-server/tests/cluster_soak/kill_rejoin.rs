//! Three-node `kill -9` rounds shared by soak legs. Each round kills the observed leader (every
//! `NODES`th round) or a follower while it is paused inside a checkpoint, requires the survivors
//! to commit, then restarts it and requires it to rejoin and commit again. A per-node counter
//! metric must grow on every node before the first kill, on each restarted node after it rejoins,
//! and on every node through the steady phase, so a run in which one node does all the work fails.
//!
//! Every wait also calls the leg's `live` check, which panics when the leg's own input or oracle
//! thread has stopped.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{
    assert_running_nodes, cluster_key_group_count, cluster_node_config,
    configured_cluster_checkpoint_timeout, custom_s3_endpoint_configured, env_u64,
    has_full_membership, observed_leader, recovery_ceiling, remaining_progress_window,
    select_follower_victim, validate_checkpoint_liveness, wait_for,
    wait_for_converged_durable_checkpoint, DurableCheckpointStatus, JoinDelivery, Node,
    ResolvedExecutable, BASE_PORT, NODES,
};

/// Grows on every node that commits checkpoints.
#[cfg(any(feature = "rabbitmq", feature = "ibmmq", feature = "mongodb-cdc"))]
pub(super) const CHECKPOINTS_COMPLETED: &str = "laminardb_checkpoints_completed_total";
/// Grows only on the nodes that read the source.
pub(super) const EVENTS_INGESTED: &str = "laminardb_events_ingested_total";

/// Add every node that has read the source since it last started.
#[cfg(any(feature = "rabbitmq", feature = "ibmmq", feature = "mongodb-cdc"))]
pub(super) fn record_consumers(nodes: &[Node], consumers: &mut BTreeSet<usize>) {
    consumers.extend(
        nodes
            .iter()
            .filter(|node| node.metric(EVENTS_INGESTED).is_some_and(|rows| rows > 0.0))
            .map(|node| node.id),
    );
}

pub(super) struct SoakSettings {
    pub delivery: JoinDelivery,
    pub interval_ms: u64,
    pub checkpoint_timeout: Duration,
    pub recovery_ceiling: Duration,
    pub kills: u64,
    pub steady: Duration,
    pub rps: u64,
    pub key_groups: u32,
}

impl SoakSettings {
    pub fn from_environment(delivery: JoinDelivery) -> Self {
        let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500);
        assert!(
            interval_ms >= 100,
            "LAMINAR_SOAK_INTERVAL_MS must be at least 100"
        );
        let recovery_ceiling = recovery_ceiling();
        let checkpoint_timeout =
            configured_cluster_checkpoint_timeout(delivery, custom_s3_endpoint_configured());
        validate_checkpoint_liveness(interval_ms, checkpoint_timeout, recovery_ceiling);
        let kills = env_u64("LAMINAR_SOAK_KILLS", 4);
        assert!(
            cfg!(debug_assertions) || kills == 0,
            "killing inside a checkpoint needs the debug-only gate of `cargo test --profile soak`"
        );
        let rps = env_u64("LAMINAR_SOAK_RPS", 400);
        assert!(rps > 0, "LAMINAR_SOAK_RPS must be greater than zero");
        Self {
            delivery,
            interval_ms,
            checkpoint_timeout,
            recovery_ceiling,
            kills,
            steady: Duration::from_secs(env_u64("LAMINAR_SOAK_SECONDS", 30)),
            rps,
            key_groups: cluster_key_group_count(),
        }
    }
}

/// Write each node's configuration: `preamble` holds the top-level keys that must
/// precede every TOML table, `workload` the source, pipeline and sink tables.
pub(super) fn cluster_nodes(
    executable: &Arc<ResolvedExecutable>,
    dir: &Path,
    log_dir: &Path,
    settings: &SoakSettings,
    checkpoint_url: &str,
    preamble: &str,
    workload: &str,
) -> Vec<Node> {
    (0..NODES)
        .map(|id| {
            let config = format!(
                "{preamble}{}{workload}",
                cluster_node_config(
                    id,
                    settings.interval_ms,
                    settings.checkpoint_timeout,
                    settings.key_groups,
                    checkpoint_url,
                    settings.delivery,
                )
            );
            let config_path = dir.join(format!("node{id}.toml"));
            std::fs::write(&config_path, config).expect("write node config");
            Node {
                id,
                executable: Arc::clone(executable),
                config_path,
                log_path: log_dir.join(format!("node{id}.log")),
                child: None,
                process_generation: 0,
                http_port: BASE_PORT + id as u16,
                fault_trigger_path: None,
                checkpoint_gate_path: Some(dir.join(format!("checkpoint-node-{id}.arm"))),
                sink_commit_gate_path: None,
            }
        })
        .collect()
}

/// Start every node, wait for one durable cluster checkpoint, and require `metric` to grow on
/// every node.
pub(super) fn boot(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    settings: &SoakSettings,
    metric: &str,
) -> DurableCheckpointStatus {
    for node in nodes.iter_mut() {
        let permit = node.verify_executable_for_spawn();
        node.spawn(permit);
        // Stagger process startup to reduce formation churn, as the join soak does.
        std::thread::sleep(Duration::from_millis(500));
    }
    wait_for(
        "every node to become ready",
        Duration::from_secs(60),
        || {
            live();
            nodes.iter_mut().all(|node| {
                node.assert_running();
                node.epoch().is_some() && node.is_ready()
            })
        },
    );
    wait_for(
        "every node to observe full cluster membership",
        Duration::from_secs(60),
        || {
            assert_running_nodes(nodes);
            live();
            has_full_membership(nodes)
        },
    );
    // A pre-join epoch can burn a full gate timeout before the first durable checkpoint.
    let deadline = Instant::now() + settings.recovery_ceiling + Duration::from_secs(60);
    let first = wait_for_converged_durable_checkpoint(nodes, deadline, "first checkpoint", None);
    wait_for(
        &format!("every node to advance {metric}"),
        remaining_progress_window(deadline, "first publication"),
        || {
            assert_running_nodes(nodes);
            live();
            nodes
                .iter()
                .all(|node| node.metric(metric).is_some_and(|value| value > 0.0))
        },
    );
    first
}

fn stable_leader(nodes: &mut [Node], live: &mut dyn FnMut()) -> usize {
    let mut candidate = None;
    let mut samples = 0_u8;
    wait_for(
        "one stable observed cluster leader",
        Duration::from_secs(30),
        || {
            assert_running_nodes(nodes);
            live();
            let leader = observed_leader(nodes);
            samples = if leader.is_some() && leader == candidate {
                samples + 1
            } else {
                1
            };
            candidate = leader;
            candidate.is_some() && samples >= 3
        },
    );
    candidate.expect("stable leader wait completed without a leader")
}

/// Kill the leader (every `NODES`th round) or a follower while it is paused inside a checkpoint,
/// require the survivors to commit, then restart it and require it to advance `metric` and commit
/// again. Returns the restarted node and the checkpoint committed after it rejoined.
pub(super) fn kill_round(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    round: u64,
    killed_followers: &mut BTreeSet<usize>,
    settings: &SoakSettings,
    metric: &str,
    latest: DurableCheckpointStatus,
) -> (usize, DurableCheckpointStatus) {
    let leader = stable_leader(nodes, live);
    let (victim, role) = if round.is_multiple_of(NODES as u64) {
        (leader, "leader")
    } else {
        let rotation = usize::try_from(round).unwrap_or(usize::MAX);
        let victim = select_follower_victim(leader, killed_followers, rotation);
        (victim, "follower")
    };
    nodes[victim].arm_checkpoint_kill(role);
    // A spontaneous recovery round exposes no checkpoint to arm until it completes.
    wait_for(
        "the selected node to pause inside its armed checkpoint phase",
        settings.recovery_ceiling + Duration::from_secs(45),
        || {
            assert_running_nodes(nodes);
            live();
            nodes[victim].checkpoint_kill_ready(role)
        },
    );
    let failover_deadline = Instant::now() + settings.recovery_ceiling;
    nodes[victim].kill9();
    nodes[victim].disarm_checkpoint_kill();
    if role == "follower" {
        killed_followers.insert(victim);
    }
    eprintln!("soak round {round}: kill -9 delivered to {role} node {victim} inside a checkpoint");
    let survived = wait_for_converged_durable_checkpoint(
        nodes,
        failover_deadline,
        "survivor progress after kill",
        Some(latest),
    );

    let permit = nodes[victim].verify_executable_for_spawn();
    let rejoin_deadline = Instant::now() + settings.recovery_ceiling;
    nodes[victim].spawn(permit);
    wait_for(
        &format!("the restarted node to rejoin and advance {metric}"),
        remaining_progress_window(rejoin_deadline, "restarted-node recovery"),
        || {
            assert_running_nodes(nodes);
            live();
            has_full_membership(nodes)
                && nodes[victim]
                    .metric(metric)
                    .is_some_and(|value| value > 0.0)
        },
    );
    let rejoined = wait_for_converged_durable_checkpoint(
        nodes,
        rejoin_deadline,
        "progress after rejoin",
        Some(survived),
    );
    eprintln!(
        "soak round {round}: node {victim} rejoined; checkpoint {} committed",
        rejoined.checkpoint_id
    );
    (victim, rejoined)
}

fn counter(node: &Node, metric: &str) -> f64 {
    let mut value = None;
    wait_for(
        &format!("node{} to expose {metric}", node.id),
        Duration::from_secs(10),
        || {
            value = node.metric(metric);
            value.is_some()
        },
    );
    value.expect("metric wait completed without a value")
}

/// Run the steady phase and require `metric` to grow on every node through it, so a node that
/// worked once and then stalled cannot pass.
pub(super) fn steady_phase(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    steady: Duration,
    metric: &str,
) {
    let before: Vec<f64> = nodes.iter().map(|node| counter(node, metric)).collect();
    let until = Instant::now() + steady;
    while Instant::now() < until {
        assert_running_nodes(nodes);
        live();
        std::thread::sleep(Duration::from_millis(500));
    }
    let after: Vec<f64> = nodes.iter().map(|node| counter(node, metric)).collect();
    let during: Vec<f64> = after.iter().zip(&before).map(|(a, b)| a - b).collect();
    eprintln!("soak: {metric} per node during the steady phase: {during:?}");
    assert!(
        during.iter().all(|value| *value > 0.0),
        "every node must keep advancing {metric} through the steady phase: {during:?}"
    );
}
