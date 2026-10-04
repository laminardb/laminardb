//! Durable submission through the existing database-owned topology driver.

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{
    CatalogManifestRef, ClusterController, LegacyTopologyBaseline, TopologyAdmissionPlan,
    TopologyAdoptionOutcome, TopologyError, TOPOLOGY_SUBMISSION_PROTOCOL_VERSION,
};
use laminar_sql::parser::StreamingStatement;

use super::{DbError, LaminarDB, TopologyAdmissionStatus, TopologyOperationId, TopologyVersion};

pub(super) const SUBMISSION_TIMEOUT: Duration = Duration::from_secs(45);

/// One atomic candidate graph and the request identity reused on every retry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTopologyRequest {
    /// Nonzero caller UUID; a changed payload must use a new identity.
    pub operation_id: TopologyOperationId,
    /// Exact committed parent, checked atomically with admission.
    pub expected_parent_version: TopologyVersion,
    /// Ordered individual DDL statements; the entire candidate is validated before admission.
    pub statements: Vec<String>,
}

/// Explicit adoption of an existing sealed inventory after a coordinated binary upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTopologyAdoptionRequest {
    /// Nonzero caller UUID, preserved by retries.
    pub operation_id: TopologyOperationId,
    /// Exact original manifest; its historical bytes and generations are preserved.
    pub expected_manifest: CatalogManifestRef,
    /// Existing checkpoint/control deployment UUID; adoption never initializes it.
    pub expected_deployment_id: String,
    /// Operator assertion that every process has completed the coordinated binary upgrade.
    pub coordinated_upgrade_complete: bool,
}

impl LaminarDB {
    /// Admit an atomic topology candidate and return its durable operation receipt.
    ///
    /// The existing recovery monitor owns all subsequent phases. Cancellation or a lost response
    /// cannot revoke admission. This response does not claim target activation; query status with
    /// the same UUID. Identical retries return the recorded operation before compiling again.
    /// A follower forwards at most once to the durable leader within the same 45 second budget.
    ///
    /// # Errors
    /// Rejects changed payloads, stale parents, unsupported plans, missing live coordinator,
    /// unresolved authority, compiler contention and bounded deadlines. A timeout is uncertain:
    /// reread status and retry the same identity rather than allocate a replacement request.
    pub async fn submit_cluster_topology_change(
        &self,
        request: &ClusterTopologyRequest,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        self.submit_cluster_topology_until(request, SUBMISSION_TIMEOUT, true)
            .await
    }

    /// One-hop HTTP receiver; never forwards a previously forwarded submission again.
    ///
    /// # Errors
    /// Uses the ordinary admission rules and rejects a receiver that lost durable leadership.
    #[doc(hidden)]
    pub async fn submit_cluster_topology_forwarded(
        &self,
        request: &ClusterTopologyRequest,
        remaining: Duration,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        self.submit_cluster_topology_until(request, remaining.min(SUBMISSION_TIMEOUT), false)
            .await
    }

    async fn submit_cluster_topology_until(
        &self,
        request: &ClusterTopologyRequest,
        timeout: Duration,
        allow_forwarding: bool,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        if timeout.is_zero() {
            return Err(TopologyError::Contended.into());
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(TopologyError::Contended)?;
        tokio::time::timeout_at(
            deadline,
            Box::pin(async {
                super::planning::validate_request_bounds(&request.statements)?;
                let controller = self.topology_submission_controller()?;
                let authority = controller
                    .checkpoint_authority()
                    .map_err(|error| TopologyError::Protocol(error.to_string()))?;
                if !allow_forwarding {
                    self.topology_submission_route(&controller, false).await?;
                }
                // Lookup precedes running/parent/compiler gates. The original terminal result remains
                // available after the cluster has moved to a later topology or is recovering.
                if authority
                    .topology_operation_status(request.operation_id)
                    .await?
                    .is_some()
                {
                    let (operation, plan, _target, descriptor) = authority
                        .topology_preparation_input(request.operation_id)
                        .await?;
                    if plan.protocol_version != TOPOLOGY_SUBMISSION_PROTOCOL_VERSION
                        || plan.expected_parent != request.expected_parent_version
                        || descriptor.statements != request.statements
                    {
                        return Err(TopologyError::Conflict(
                            "operation identity was already bound to another payload".into(),
                        )
                        .into());
                    }
                    return Ok(operation);
                }
                self.ensure_topology_submission_available()?;
                if let Some(address) = self
                    .topology_submission_route(&controller, allow_forwarding)
                    .await?
                {
                    let operation: TopologyAdmissionStatus = self
                        .forward_topology_request(
                            &address,
                            "/api/v1/cluster/topology/operations",
                            request,
                            deadline,
                        )
                        .await?;
                    if operation.operation_id != request.operation_id {
                        return Err(TopologyError::Invalid(
                            "leader receipt belongs to another operation".into(),
                        )
                        .into());
                    }
                    return Ok(operation);
                }
                let before = controller
                    .try_live_local_process_authority_identity()
                    .map_err(|_| TopologyError::Fenced)?;
                let proof = controller
                    .capture_leader_proof()
                    .ok_or(TopologyError::Fenced)?;
                let (descriptor, target) = Box::pin(self.plan_cluster_topology_change(
                    request.expected_parent_version,
                    &request.statements,
                ))
                .await?;
                let assignments = controller.snapshot_store().ok_or_else(|| {
                    TopologyError::Protocol(
                        "submission requires the configured assignment authority".into(),
                    )
                })?;
                let assignment = assignments
                    .load()
                    .await
                    .map_err(|error| TopologyError::Protocol(error.to_string()))?
                    .ok_or_else(|| {
                        TopologyError::Conflict("assignment inventory is uninitialized".into())
                    })?
                    .assignment_fence()
                    .map_err(|error| TopologyError::Invalid(error.to_string()))?;
                if assignment.participant_incarnation(before.participant.node_id)
                    != Some(before.participant.boot_incarnation)
                    || controller
                        .checkpoint_assignment_fence(assignment.assignment_version)
                        .as_ref()
                        != Some(&assignment)
                    || controller.try_live_local_process_authority_identity().ok() != Some(before)
                    || !controller.proof_is_live(&proof)
                {
                    return Err(TopologyError::Fenced.into());
                }
                let compatibility = authority.stage_topology_compatibility(&descriptor).await?;
                let plan = TopologyAdmissionPlan {
                    protocol_version: TOPOLOGY_SUBMISSION_PROTOCOL_VERSION,
                    operation_id: request.operation_id,
                    expected_parent: request.expected_parent_version,
                    parent_manifest: descriptor.parent_manifest,
                    target_manifest: descriptor.target_manifest,
                    assignment,
                    compatibility: Some(compatibility),
                };
                let admitted = loop {
                    self.ensure_topology_submission_available()?;
                    if !controller.proof_is_live(&proof)
                        || controller.try_live_local_process_authority_identity().ok()
                            != Some(before)
                    {
                        return Err(TopologyError::Fenced.into());
                    }
                    match authority
                        .admit_topology_plan(&proof, assignments, &plan, &target)
                        .await
                    {
                        Ok(admitted) => break admitted,
                        Err(TopologyError::Contended) => {
                            // Do not rebuild or change the operation UUID after checkpoint/CAS
                            // contention. The outer timeout bounds this control-path retry, and
                            // authority rereads resolve a lost successful append response.
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                tracing::info!(operation = %request.operation_id.get(), phase = ?admitted.phase,
                "admitted durable topology request; coordinator owns further progress");
                Ok(admitted)
            }),
        )
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    /// Explicitly adopt topology one without rewriting the existing catalog or checkpoint storage.
    ///
    /// # Errors
    /// Requires the coordinated-upgrade assertion, exact existing deployment/manifest, live
    /// leadership and the existing bounded adoption CAS. A follower forwards at most once.
    pub async fn adopt_cluster_topology(
        &self,
        request: &ClusterTopologyAdoptionRequest,
    ) -> Result<LegacyTopologyBaseline, DbError> {
        self.adopt_cluster_topology_until(request, SUBMISSION_TIMEOUT, true)
            .await
    }

    /// One-hop HTTP adoption receiver using the caller's remaining bounded budget.
    ///
    /// # Errors
    /// Uses explicit adoption rules and rejects lost leadership or invalid evidence.
    #[doc(hidden)]
    pub async fn adopt_cluster_topology_forwarded(
        &self,
        request: &ClusterTopologyAdoptionRequest,
        remaining: Duration,
    ) -> Result<LegacyTopologyBaseline, DbError> {
        self.adopt_cluster_topology_until(request, remaining.min(SUBMISSION_TIMEOUT), false)
            .await
    }

    async fn adopt_cluster_topology_until(
        &self,
        request: &ClusterTopologyAdoptionRequest,
        timeout: Duration,
        allow_forwarding: bool,
    ) -> Result<LegacyTopologyBaseline, DbError> {
        if timeout.is_zero() {
            return Err(TopologyError::Contended.into());
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(TopologyError::Contended)?;
        tokio::time::timeout_at(deadline, async {
            if !request.coordinated_upgrade_complete {
                return Err(TopologyError::Protocol("complete the coordinated binary upgrade on every process before explicit legacy adoption".into()).into());
            }
            let deployment = uuid::Uuid::parse_str(&request.expected_deployment_id)
                .map_err(|_| TopologyError::Invalid("deployment must be a canonical nonzero UUID".into()))?;
            if deployment.is_nil() || deployment.to_string() != request.expected_deployment_id {
                return Err(TopologyError::Invalid("deployment must be a canonical nonzero UUID".into()).into());
            }
            let controller = self.topology_submission_controller()?;
            self.ensure_topology_preparation_available()?;
            if let Some(address) = self.topology_submission_route(&controller, allow_forwarding).await? {
                let baseline: LegacyTopologyBaseline = self.forward_topology_request(&address, "/api/v1/cluster/topology/adopt", request, deadline).await?;
                if baseline.manifest != request.expected_manifest
                    || baseline.deployment_id != request.expected_deployment_id {
                    return Err(TopologyError::Invalid("leader adoption receipt differs from the exact request".into()).into());
                }
                return Ok(baseline);
            }
            let authority = controller.checkpoint_authority()
                .map_err(|error| TopologyError::Protocol(error.to_string()))?;
            let proof = controller.capture_leader_proof().ok_or(TopologyError::Fenced)?;
            let catalog = self.catalog_manifest_store.lock().clone()
                .ok_or_else(|| TopologyError::Protocol("adoption requires configured catalog authority".into()))?;
            let (manifest, _) = catalog.load_with_topology().await?
                .ok_or_else(|| TopologyError::Conflict("legacy catalog is not sealed".into()))?;
            if manifest.reference().map_err(TopologyError::from)? != request.expected_manifest
                || self.catalog_manifest_inventory()? != manifest.entries {
                return Err(TopologyError::Conflict("adoption requires the exact sealed, replayed live catalog".into()).into());
            }
            let baseline = match authority.adopt_legacy_topology(&proof, request.operation_id,
                &request.expected_manifest, &request.expected_deployment_id).await? {
                TopologyAdoptionOutcome::Created(baseline) | TopologyAdoptionOutcome::Existing(baseline) => baseline,
            };
            self.ensure_topology_preparation_available()?;
            if !controller.proof_is_live(&proof) {
                return Err(TopologyError::Fenced.into());
            }
            *self.replayed_topology_version.lock() = Some(TopologyVersion::LEGACY_BASELINE);
            Ok(baseline)
        }).await.map_err(|_| TopologyError::Contended)?
    }

    pub(in crate::db) async fn submit_cluster_topology_sql(
        &self,
        sql: &str,
        statement: &StreamingStatement,
    ) -> Result<crate::handle::ExecuteResult, DbError> {
        let (name, _, _) = super::catalog_changes::statement_identity(self, sql, statement)?;
        let parent = self
            .cluster_topology_status()
            .await?
            .committed_version
            .ok_or_else(|| {
                TopologyError::Protocol(
                    "explicit legacy topology adoption is required before runtime DDL".into(),
                )
            })?;
        let request = ClusterTopologyRequest {
            operation_id: uuid::Uuid::new_v4().try_into()?,
            expected_parent_version: parent,
            statements: vec![sql.to_owned()],
        };
        let status = Box::pin(self.submit_cluster_topology_change(&request))
            .await
            .map_err(|source| {
                if matches!(
                    &source,
                    DbError::Topology(
                        TopologyError::Contended
                            | TopologyError::Authority(_)
                            | TopologyError::Fenced
                    )
                ) {
                    DbError::TopologySubmission {
                        operation_id: request.operation_id,
                        source: Box::new(source),
                    }
                } else {
                    source
                }
            })?;
        Ok(crate::handle::ExecuteResult::Ddl(crate::handle::DdlInfo {
            statement_type: "TOPOLOGY MIGRATION".into(),
            object_name: name,
            applied: false,
            topology_operation: Some(Box::new(status)),
        }))
    }

    fn topology_submission_controller(&self) -> Result<Arc<ClusterController>, DbError> {
        if !self.is_cluster_runtime() {
            return Err(TopologyError::Unsupported(
                "topology submission requires cluster mode".into(),
            )
            .into());
        }
        self.cluster_controller.lock().clone().ok_or_else(|| {
            TopologyError::Protocol("submission requires the configured cluster controller".into())
                .into()
        })
    }

    fn ensure_topology_submission_available(&self) -> Result<(), DbError> {
        self.ensure_topology_preparation_available()?;
        if self.cluster_intake_fenced()
            || self.force_ckpt_tx.lock().is_none()
            || self
                .recovery_monitor
                .lock()
                .as_ref()
                .is_none_or(tokio::task::JoinHandle::is_finished)
        {
            return Err(TopologyError::Conflict("submission requires released parent intake and the live checkpoint/recovery coordinator".into()).into());
        }
        Ok(())
    }

    async fn topology_submission_route(
        &self,
        controller: &ClusterController,
        allow_forwarding: bool,
    ) -> Result<Option<String>, DbError> {
        let authority = controller
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        let lease = authority
            .load()
            .await
            .map_err(TopologyError::from)?
            .ok_or(TopologyError::Fenced)?;
        if controller
            .capture_leader_proof()
            .as_ref()
            .is_some_and(|proof| lease.matches_proof(proof))
        {
            return Ok(None);
        }
        if !allow_forwarding || !controller.process_lease_is_live() {
            return Err(TopologyError::Fenced.into());
        }
        let watch = controller.members_watch();
        let members = watch.borrow();
        let address = members
            .iter()
            .find(|member| member.id.0 == lease.owner.node.0)
            .map(|member| member.rpc_address.trim())
            .filter(|address| !address.is_empty())
            .ok_or_else(|| {
                TopologyError::Conflict(
                    "durable leader HTTP address is unresolved; retry the same request identity"
                        .into(),
                )
            })?;
        Ok(Some(address.to_owned()))
    }
}
