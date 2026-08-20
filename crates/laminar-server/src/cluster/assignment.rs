//! Startup assignment ownership: roster verification, rendezvous genesis,
//! CAS resolution, certification, and startup leader-authority convergence.
//!
//! Responsibility: decide which vnode assignment this boot may act on. Verify
//! the advertised startup roster against durable process leases, resolve the
//! initial assignment snapshot (create-only CAS), certify the exact local
//! assignment fence before the pipeline starts, and wait for durable leader
//! authority to converge with the certified assignment.
//!
//! Ordering constraints:
//! - the process deadline is checked before and after every durable
//!   assignment operation; a fenced process never creates or preinstalls;
//! - an existing or racing different formation boots unassigned for the
//!   audited pre-start adoption path; only the exact same v1 genesis
//!   formation may preinstall;
//! - catalog bootstrap authority (durable lease or observed active peer) and
//!   leader-authority convergence share the controller's watch set.

use std::sync::Arc;

use tracing::info;

use laminar_core::cluster::discovery::{NodeId, NodeState};

use super::control_kv::OBJECT_STORE_CONTROL_IO_TIMEOUT;
use super::{ClusterStartupError, PROCESS_INCARNATION_TAG};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CatalogStartupAuthority {
    DurableLease,
    ActivePeer,
}

pub(super) async fn wait_for_catalog_startup_authority(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    timeout: std::time::Duration,
) -> Result<CatalogStartupAuthority, String> {
    let mut grants = controller
        .leader_grant_watch()
        .ok_or_else(|| "durable leader lease fencing is not installed".to_string())?;
    let mut members = controller.members_watch();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if controller.capture_catalog_bootstrap_proof().is_some() {
            return Ok(CatalogStartupAuthority::DurableLease);
        }
        if members.borrow().iter().any(|member| {
            member.id != controller.instance_id() && matches!(member.state, NodeState::Active)
        }) {
            return Ok(CatalogStartupAuthority::ActivePeer);
        }

        tokio::select! {
            () = &mut deadline => {
                return Err(format!(
                    "timed out after {timeout:?} waiting for the durable catalog bootstrap lease or an active peer"
                ));
            }
            changed = grants.changed() => {
                if changed.is_err() {
                    return Err("durable leader lease manager stopped during catalog bootstrap".into());
                }
            }
            changed = members.changed() => {
                if changed.is_err() {
                    return Err("membership discovery stopped during catalog bootstrap".into());
                }
            }
        }
    }
}

pub(super) const STARTUP_ASSIGNMENT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(60);
const STARTUP_LEADER_AUTHORITY_MIN_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(25);
const STARTUP_LEADER_AUTHORITY_MAX_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(250);
const STARTUP_LEADER_AUTHORITY_MAX_SLEEP: std::time::Duration =
    std::time::Duration::from_millis(375);
const STARTUP_PROCESS_LEASE_MIN_BACKOFF: std::time::Duration = std::time::Duration::from_millis(25);
const STARTUP_PROCESS_LEASE_MAX_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(250);

pub(super) fn exact_startup_assignment_fence(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
) -> Option<laminar_core::checkpoint::CheckpointAssignmentFence> {
    let assignment = registry.versioned_snapshot();
    if assignment.version() == 0 {
        return None;
    }
    let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
    let fence = controller.checkpoint_assignment_fence(assignment.version())?;
    if !fence.matches_owner_map(&owners) {
        return None;
    }
    let local = controller.instance_id().0;
    let owns = owners.contains(&local);
    let local_incarnation = fence.participant_incarnation(local);
    ((owns && local_incarnation == Some(controller.recovery_incarnation()))
        || (!owns && local_incarnation.is_none()))
    .then_some(fence)
}

pub(super) async fn audit_stable_startup_assignment(
    controller: &laminar_core::cluster::control::ClusterController,
    store: &laminar_core::cluster::control::AssignmentSnapshotStore,
    registry: &laminar_core::state::VnodeRegistry,
    timeout: std::time::Duration,
) -> Result<(), ClusterStartupError> {
    let expected = exact_startup_assignment_fence(controller, registry).ok_or_else(|| {
        ClusterStartupError::EngineConstruction(
            "startup assignment is not certified for the current process".into(),
        )
    })?;
    tokio::time::timeout(timeout, async {
        let head = store
            .load()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "durable assignment head is missing".to_string())?;
        let committed =
            laminar_db::rebalance::startup_committed_assignment(store, Some(controller), head)
                .await?;
        if committed.draining
            || committed.version != expected.assignment_version
            || committed
                .assignment_fence()
                .map_err(|error| error.to_string())?
                != expected
        {
            return Err(format!(
                "durable assignment {} does not match certified startup assignment {}",
                committed.version, expected.assignment_version
            ));
        }
        let confirmed = store
            .load()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "durable assignment head disappeared during startup audit".to_string()
            })?;
        if confirmed != committed {
            return Err(format!(
                "durable assignment changed from {} to {} during startup audit",
                committed.version, confirmed.version
            ));
        }
        if exact_startup_assignment_fence(controller, registry).as_ref() != Some(&expected)
            || !controller.process_lease_is_live()
        {
            return Err(
                "startup assignment certificate or process authority changed during durable audit"
                    .into(),
            );
        }
        Ok(())
    })
    .await
    .map_err(|_| {
        ClusterStartupError::EngineConstruction(format!(
            "durable startup assignment audit exceeded {timeout:?}"
        ))
    })?
    .map_err(|error| {
        ClusterStartupError::EngineConstruction(format!(
            "durable startup assignment audit failed: {error}"
        ))
    })
}

fn startup_leader_audit_deadline(overall: tokio::time::Instant) -> tokio::time::Instant {
    // The exact controller audit serializes five bounded control operations under one deadline:
    // durable head, live proof, process term, live proof, and final durable head.
    let audit_budget = OBJECT_STORE_CONTROL_IO_TIMEOUT
        .checked_mul(5)
        .expect("the fixed startup authority audit budget fits Duration");
    tokio::time::Instant::now()
        .checked_add(audit_budget)
        .map_or(overall, |deadline| deadline.min(overall))
}

pub(super) fn startup_leader_authority_timeout(
    config: laminar_core::cluster::control::LeaderLeaseConfig,
    control_io: std::time::Duration,
) -> Option<std::time::Duration> {
    // Initial attempt, full rival observation, and takeover can each consume one TTL. The
    // successful remote audit then serializes five bounded operations: durable head, live RPC,
    // process-term verification, live RPC, and final durable head.
    config
        .ttl
        .checked_mul(3)?
        .checked_add(config.renew_interval)?
        .checked_add(control_io.checked_mul(5)?)?
        .checked_add(STARTUP_LEADER_AUTHORITY_MAX_SLEEP)
}

pub(super) async fn wait_for_startup_leader_authority(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
    timeout: std::time::Duration,
) -> Result<(), ClusterStartupError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| {
            ClusterStartupError::EngineConstruction(
                "leader authority convergence deadline exceeds the monotonic timer range".into(),
            )
        })?;
    let mut last_expected = None;
    let mut last_audit_error = None;
    let mut backoff = STARTUP_LEADER_AUTHORITY_MIN_BACKOFF;
    let mut previous_candidate = None;
    let wait = async {
        loop {
            let fence = exact_startup_assignment_fence(controller, registry);
            let candidate = fence.as_ref().and_then(|fence| {
                let leader = controller.current_leader()?;
                let participant = fence
                    .participants
                    .iter()
                    .find(|participant| participant.node_id == leader.0)?;
                Some((
                    fence.assignment_version,
                    fence.assignment_digest,
                    *participant,
                ))
            });
            if candidate != previous_candidate {
                backoff = STARTUP_LEADER_AUTHORITY_MIN_BACKOFF;
                previous_candidate = candidate;
            }
            if let Some((version, _, participant)) = candidate {
                last_expected = Some(format!(
                    "assignment {version} candidate {} boot {}",
                    participant.node_id, participant.boot_incarnation
                ));
            } else if let Some(fence) = fence.as_ref() {
                last_expected = Some(format!(
                    "assignment {} has no certified current leader participant",
                    fence.assignment_version
                ));
            } else {
                last_expected = Some("no exact assignment fence is currently installed".into());
            }

            if let Some(fence) = fence {
                match controller
                    .audit_assignment_leader_authority(
                        &fence,
                        None,
                        startup_leader_audit_deadline(deadline),
                    )
                    .await
                {
                    Ok(_) => return,
                    Err(error) => last_audit_error = Some(error),
                }
            } else {
                last_audit_error = Some("no exact assignment fence is currently installed".into());
            }
            use rand::RngExt as _;
            let base_ms = u64::try_from(backoff.as_millis()).unwrap_or(250);
            let jitter_ms = rand::rng().random_range(0..=base_ms / 2);
            tokio::time::sleep(backoff + std::time::Duration::from_millis(jitter_ms)).await;
            backoff = backoff
                .checked_mul(2)
                .unwrap_or(STARTUP_LEADER_AUTHORITY_MAX_BACKOFF)
                .min(STARTUP_LEADER_AUTHORITY_MAX_BACKOFF);
        }
    };
    if tokio::time::timeout_at(deadline, wait).await.is_ok() {
        return Ok(());
    }

    let expected =
        last_expected.unwrap_or_else(|| "no assignment-certified leader candidate".to_string());
    let audit_error = last_audit_error
        .map(|error| format!("; last authority audit failed: {error}"))
        .unwrap_or_default();
    Err(ClusterStartupError::EngineConstruction(format!(
        "durable leader authority did not converge with a live certified grant within {timeout:?}: {expected}{audit_error}"
    )))
}

pub(super) async fn wait_for_startup_assignment_fence(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
    rebalance_tasks: &[tokio::task::JoinHandle<()>],
) -> Result<(), ClusterStartupError> {
    let mut fence_rx = controller.checkpoint_assignment_watch();
    let mut members_rx = controller.members_watch();
    let wait = async {
        loop {
            if exact_startup_assignment_fence(controller, registry).is_some() {
                return Ok(());
            }
            if rebalance_tasks
                .iter()
                .any(tokio::task::JoinHandle::is_finished)
            {
                return Err(ClusterStartupError::EngineConstruction(
                    "bootstrap assignment task exited before certification".into(),
                ));
            }
            tokio::select! {
                result = fence_rx.changed() => result.map_err(|_| {
                    ClusterStartupError::EngineConstruction(
                        "cluster assignment certification channel closed during startup".into(),
                    )
                })?,
                result = members_rx.changed() => result.map_err(|_| {
                    ClusterStartupError::EngineConstruction(
                        "cluster membership channel closed during assignment certification".into(),
                    )
                })?,
                () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {},
            }
        }
    };
    tokio::time::timeout(STARTUP_ASSIGNMENT_TIMEOUT, wait)
        .await
        .map_err(|_| {
            ClusterStartupError::EngineConstruction(format!(
                "cluster assignment {} was not certified within {STARTUP_ASSIGNMENT_TIMEOUT:?}",
                registry.assignment_version()
            ))
        })?
}

/// Load one durable startup lease, retrying only object-store I/O within the roster deadline.
async fn load_startup_process_lease_until(
    store: &Arc<dyn object_store::ObjectStore>,
    node: NodeId,
    process_lease_ttl_ms: i64,
    deadline: tokio::time::Instant,
) -> Result<Option<laminar_core::cluster::control::ProcessLease>, ClusterStartupError> {
    use laminar_core::cluster::control::{ProcessLeaseError, ProcessLeaseStore};
    use rand::RngExt as _;

    let lease_store = ProcessLeaseStore::new(Arc::clone(store), node, process_lease_ttl_ms);
    let mut backoff = STARTUP_PROCESS_LEASE_MIN_BACKOFF;
    let mut last_io_error = None;
    let deadline_error = |last_error: Option<&str>| {
        let context = last_error
            .map(|error| format!("; last object-store error: {error}"))
            .unwrap_or_default();
        ClusterStartupError::EngineConstruction(format!(
            "load process lease for node {} exceeded {STARTUP_ASSIGNMENT_TIMEOUT:?}{context}",
            node.0
        ))
    };
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(deadline_error(last_io_error.as_deref()));
        }
        let load = tokio::time::timeout_at(deadline, lease_store.load()).await;
        match load {
            Ok(Ok(lease)) => return Ok(lease),
            Ok(Err(ProcessLeaseError::Io(error))) => last_io_error = Some(error),
            Ok(Err(error)) => {
                return Err(ClusterStartupError::EngineConstruction(format!(
                    "load process lease for node {}: {error}",
                    node.0
                )));
            }
            Err(_) => return Err(deadline_error(last_io_error.as_deref())),
        }
        let now = tokio::time::Instant::now();
        let base_ms = u64::try_from(backoff.as_millis()).unwrap_or(250);
        let jitter = std::time::Duration::from_millis(rand::rng().random_range(0..=base_ms / 2));
        tokio::time::sleep_until((now + backoff + jitter).min(deadline)).await;
        backoff = backoff
            .checked_mul(2)
            .unwrap_or(STARTUP_PROCESS_LEASE_MAX_BACKOFF)
            .min(STARTUP_PROCESS_LEASE_MAX_BACKOFF);
    }
}

/// Verify advertised startup process incarnations against their durable stable-node leases.
pub(super) async fn assignment_seed_participants(
    self_id: laminar_core::state::NodeId,
    self_incarnation: uuid::Uuid,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    store: &Arc<dyn object_store::ObjectStore>,
    process_lease_ttl_ms: i64,
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    let advertised = advertised_startup_participants(self_id, self_incarnation, peers)?;
    let deadline = tokio::time::Instant::now()
        .checked_add(STARTUP_ASSIGNMENT_TIMEOUT)
        .ok_or_else(|| {
            ClusterStartupError::EngineConstruction(
                "startup process-lease audit deadline exceeds the monotonic timer range".into(),
            )
        })?;
    let mut participants = Vec::with_capacity(advertised.len());
    for participant in advertised {
        let node = NodeId(participant.node_id);
        let boot_incarnation = participant.boot_incarnation;
        let lease = load_startup_process_lease_until(store, node, process_lease_ttl_ms, deadline)
            .await?
            .ok_or_else(|| {
                ClusterStartupError::EngineConstruction(format!(
                    "node {} has no durable process lease",
                    node.0
                ))
            })?;
        if lease.owner != boot_incarnation {
            return Err(ClusterStartupError::EngineConstruction(format!(
                "node {} advertised process {} but durable lease belongs to {}",
                node.0, boot_incarnation, lease.owner
            )));
        }
        participants.push(participant);
    }
    Ok(participants)
}

pub(super) fn advertised_startup_participants(
    self_id: laminar_core::state::NodeId,
    self_incarnation: uuid::Uuid,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    let mut advertised = Vec::with_capacity(peers.len() + 1);
    advertised.push((self_id, self_incarnation));
    for peer in peers {
        let incarnation = peer
            .metadata
            .tags
            .get(PROCESS_INCARNATION_TAG)
            .ok_or_else(|| {
                ClusterStartupError::EngineConstruction(format!(
                    "peer {} did not advertise its process incarnation",
                    peer.id.0
                ))
            })?
            .parse()
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "peer {} advertised an invalid process incarnation: {error}",
                    peer.id.0
                ))
            })?;
        advertised.push((peer.id, incarnation));
    }
    advertised.sort_unstable_by_key(|(node, _)| node.0);
    if advertised.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(ClusterStartupError::EngineConstruction(
            "initial assignment participant roster contains duplicate node ids".into(),
        ));
    }
    if advertised.len() > laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS {
        return Err(ClusterStartupError::EngineConstruction(format!(
            "startup roster has {} participants; maximum is {}",
            advertised.len(),
            laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
        )));
    }
    Ok(advertised
        .into_iter()
        .map(
            |(node, boot_incarnation)| laminar_core::checkpoint::CheckpointParticipant {
                node_id: node.0,
                boot_incarnation,
            },
        )
        .collect())
}

pub(super) fn is_same_formation_genesis(
    stored: &laminar_core::cluster::control::AssignmentSnapshot,
    proposed: &laminar_core::cluster::control::AssignmentSnapshot,
) -> bool {
    if stored.version != 1
        || proposed.version != 1
        || stored.draining
        || proposed.draining
        || stored.drain_transition.is_some()
        || proposed.drain_transition.is_some()
        || stored.vnodes != proposed.vnodes
    {
        return false;
    }
    matches!(
        (stored.assignment_fence(), proposed.assignment_fence()),
        (Ok(stored), Ok(proposed)) if stored == proposed
    )
}

/// Build the immutable rendezvous proposal this boot would create: the
/// deterministic vnode map over peers plus self, the shared snapshot store,
/// and the v1 snapshot restricted to owners with certified incarnations.
fn prepare_startup_assignment_proposal(
    self_id: laminar_core::cluster::discovery::NodeId,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    vnode_count: u32,
    control_store: Arc<dyn object_store::ObjectStore>,
    startup_participants: &[laminar_core::checkpoint::CheckpointParticipant],
    process_deadline: &laminar_core::cluster::control::LeaseDeadline,
) -> Result<
    (
        Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
        laminar_core::cluster::control::AssignmentSnapshot,
    ),
    ClusterStartupError,
> {
    use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};
    use laminar_core::state::{rendezvous_assignment, NodeId};

    if !process_deadline.is_live() {
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease expired before assignment resolution".into(),
        ));
    }

    let peer_ids: Vec<NodeId> = peers
        .iter()
        .map(|p| NodeId(p.id.0))
        .chain(std::iter::once(NodeId(self_id.0)))
        .collect();
    let assignment: Arc<[NodeId]> = rendezvous_assignment(vnode_count, &peer_ids);

    let snapshot_store = Arc::new(AssignmentSnapshotStore::new(control_store));

    let owner_ids: std::collections::BTreeSet<u64> =
        assignment.iter().map(|owner| owner.0).collect();
    let owner_participants: Vec<_> = startup_participants
        .iter()
        .filter(|participant| owner_ids.contains(&participant.node_id))
        .cloned()
        .collect();
    if owner_participants.len() != owner_ids.len() {
        return Err(ClusterStartupError::EngineConstruction(
            "initial assignment has an owner without a certified process incarnation".into(),
        ));
    }
    let proposal = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&assignment),
            owner_participants,
        )
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("initial assignment snapshot: {error}"))
        })?;
    Ok((snapshot_store, proposal))
}

/// Validate a durably resolved snapshot against this boot's proposal and
/// install it: preinstall only the exact same v1 genesis formation, otherwise
/// boot unassigned for the audited pre-start adoption path.
fn install_resolved_startup_assignment(
    resolved: &laminar_core::cluster::control::AssignmentSnapshot,
    proposal: &laminar_core::cluster::control::AssignmentSnapshot,
    vnode_count: u32,
    decode_context: &'static str,
    preinstalled_message: &'static str,
    unassigned_message: &'static str,
) -> Result<Arc<laminar_core::state::VnodeRegistry>, ClusterStartupError> {
    use laminar_core::state::VnodeRegistry;

    let assignment = resolved.to_vnode_vec(vnode_count).map_err(|error| {
        ClusterStartupError::EngineConstruction(format!("{decode_context}: {error}"))
    })?;
    let registry = VnodeRegistry::new_unassigned(vnode_count);
    if is_same_formation_genesis(resolved, proposal) {
        registry.set_assignment_and_version(assignment.into(), resolved.version);
        info!(
            stored_version = resolved.version,
            "{}", preinstalled_message
        );
    } else {
        info!(stored_version = resolved.version, "{}", unassigned_message);
    }
    Ok(Arc::new(registry))
}

/// Resolve the boot-time vnode registry and shared assignment store.
/// Existing clusters boot unassigned and adopt the audited committed head during the pre-start
/// bootstrap control-plane phase.
/// A new cluster CAS-creates its rendezvous assignment; peers with that exact owner formation may
/// also preinstall the winner when its v1 map and process-incarnation roster match their proposal.
pub(super) async fn resolve_vnode_assignment(
    self_id: laminar_core::cluster::discovery::NodeId,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    vnode_count: u32,
    control_store: Arc<dyn object_store::ObjectStore>,
    startup_participants: &[laminar_core::checkpoint::CheckpointParticipant],
    process_deadline: &laminar_core::cluster::control::LeaseDeadline,
) -> Result<
    (
        Arc<laminar_core::state::VnodeRegistry>,
        Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    ),
    ClusterStartupError,
> {
    let (snapshot_store, proposal) = prepare_startup_assignment_proposal(
        self_id,
        peers,
        vnode_count,
        control_store,
        startup_participants,
        process_deadline,
    )?;

    // Snapshot exists → restart or joiner. Boot owning nothing: the stored snapshot may be
    // stale (a shed can race the restart), and acting on assumed ownership bypasses the adopt
    // protocol. The sole exception is the exact v1 proposal another member of this owner
    // formation already created. An owner restart changes the process-incarnation fence and
    // remains unassigned; a new process outside the owner set can only preinstall a zero-owner
    // view of the exact existing map.
    if let Some(existing) = snapshot_store
        .load()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot load: {e}")))?
    {
        if !process_deadline.is_live() {
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease expired while loading the assignment".into(),
            ));
        }
        let registry = install_resolved_startup_assignment(
            &existing,
            &proposal,
            vnode_count,
            "stored assignment",
            "preinstalled same-formation initial assignment",
            "found stored assignment snapshot; booting unassigned for audited pre-start adoption",
        )?;
        return Ok((registry, snapshot_store));
    }

    // Nothing stored yet — propose ours and CAS-create. A racing peer may win; preinstall that
    // winner only when it is the exact same genesis formation, matching the existing-head path.
    if !process_deadline.is_live() {
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease expired before the initial assignment CAS".into(),
        ));
    }
    let winner = match snapshot_store
        .save_if_absent(&proposal)
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot save: {e}")))?
    {
        Some(w) => {
            info!("Created assignment snapshot v{}", w.version);
            w
        }
        None => {
            let w = snapshot_store
                .load()
                .await
                .map_err(|e| {
                    ClusterStartupError::EngineConstruction(format!("snapshot re-load: {e}"))
                })?
                .ok_or_else(|| {
                    ClusterStartupError::EngineConstruction(
                        "snapshot CAS lost but re-load returned None".into(),
                    )
                })?;
            info!("Observed snapshot v{} after CAS race", w.version);
            w
        }
    };
    if !process_deadline.is_live() {
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease expired while resolving the assignment winner".into(),
        ));
    }
    let registry = install_resolved_startup_assignment(
        &winner,
        &proposal,
        vnode_count,
        "winning assignment",
        "preinstalled same-formation initial assignment after CAS race",
        "CAS race selected a different formation; booting unassigned for audited pre-start adoption",
    )?;
    Ok((registry, snapshot_store))
}
