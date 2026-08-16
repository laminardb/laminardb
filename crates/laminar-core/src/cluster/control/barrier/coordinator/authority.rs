//! Coordinator construction, authority binding, server ownership, and leader proof confirmation.

use super::super::{
    abort_grpc_tasks, barrier_v1, encode_barrier_endpoint, evict_barrier_client,
    get_barrier_client, leader_proof_ack_matches, leader_proof_to_wire, merge_direct_announcement,
    validate_announcement_attempt, watch, AnnouncementPublicationState, Arc, AtomicBool,
    BarrierAnnouncement, BarrierClientResolutionError, BarrierCoordinator, BarrierIdentity,
    BarrierProcessIdentity, ClusterKv, ExpectedBarrierProcess, FxHashMap, GrpcBarrierServer,
    GrpcState, LocalLeaderProofProvider, NodeId, NodeInfo, Phase, PrepareAckState,
    BARRIER_ADDR_KEY, MAX_RETAINED_BARRIER_IDENTITIES,
};

impl BarrierCoordinator {
    /// Wrap a KV implementation.
    #[must_use]
    pub fn new(kv: Arc<dyn ClusterKv>) -> Self {
        Self {
            kv,
            publication: tokio::sync::Mutex::new(AnnouncementPublicationState::default()),
            #[cfg(feature = "cluster")]
            grpc: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            prepare_observed_at: parking_lot::Mutex::new(FxHashMap::default()),
            #[cfg(feature = "cluster")]
            leader_election: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            leader_lease_store: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            local_process: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "cluster")]
            unbound_endpoint_started: parking_lot::Mutex::new(false),
            #[cfg(feature = "cluster")]
            process_lease_deadline: Arc::new(std::sync::OnceLock::new()),
        }
    }

    #[cfg(feature = "cluster")]
    pub(super) fn require_live_bound_process_lease(&self) -> Result<(), String> {
        if self.local_process.get().is_none() {
            return Ok(());
        }
        let deadline = self
            .process_lease_deadline
            .get()
            .ok_or_else(|| "process lease deadline is not installed".to_string())?;
        if !deadline.is_live() {
            return Err("process lease deadline has expired".into());
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(super) fn claim_endpoint_process(&self) -> Option<BarrierProcessIdentity> {
        let mut unbound_endpoint_started = self.unbound_endpoint_started.lock();
        let process = self.local_process.get().copied();
        if process.is_none() {
            *unbound_endpoint_started = true;
        }
        process
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_process_lease_deadline(
        &self,
        deadline: Arc<super::super::super::LeaseDeadline>,
    ) -> Result<(), String> {
        match self.process_lease_deadline.set(deadline) {
            Ok(()) => Ok(()),
            Err(deadline)
                if self
                    .process_lease_deadline
                    .get()
                    .is_some_and(|current| Arc::ptr_eq(current, &deadline)) =>
            {
                Ok(())
            }
            Err(_) => Err("process lease deadline is already installed".into()),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_local_process_lease(
        &self,
        lease: &super::super::super::ProcessLease,
    ) -> Result<(), String> {
        let process = BarrierProcessIdentity::from_process_lease(lease)?;
        let unbound_endpoint_started = self.unbound_endpoint_started.lock();
        if *unbound_endpoint_started {
            return Err(
                "an assignment-less cluster control endpoint cannot be promoted in place".into(),
            );
        }
        match self.local_process.set(process) {
            Ok(()) => Ok(()),
            Err(_) if self.local_process.get() == Some(&process) => Ok(()),
            Err(_) => Err("cluster control endpoint process identity is already installed".into()),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_local_leader_proof_provider(&self, provider: LocalLeaderProofProvider) {
        *self.local_leader_proof.lock() = Some(provider);
    }

    /// Install the durable authority used to validate clustered reversible barrier phases.
    /// Without it, clustered `Prepare` and `Aligned` traffic fails closed.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_store(&self, store: Arc<super::super::super::LeaderLeaseStore>) {
        *self.leader_lease_store.lock() = Some(store);
    }

    /// Exact durable authority installed for clustered barriers and checkpoint decisions.
    ///
    /// Embedded and single-node runtimes do not call this path. A cluster runtime that omitted
    /// authority wiring fails closed instead of falling back to standalone outcome objects.
    ///
    /// # Errors
    /// Returns `NotConfigured` when durable cluster checkpoint authority is not installed.
    #[cfg(feature = "cluster")]
    pub fn checkpoint_authority(
        &self,
    ) -> Result<
        Arc<super::super::super::LeaderLeaseStore>,
        super::super::super::ClusterCheckpointAuthorityError,
    > {
        self.leader_lease_store
            .lock()
            .clone()
            .ok_or(super::super::super::ClusterCheckpointAuthorityError::NotConfigured)
    }

    #[cfg(feature = "cluster")]
    pub(in crate::cluster::control::barrier) async fn validate_reversible_announcement(
        &self,
        ann: &BarrierAnnouncement,
    ) -> Result<(), String> {
        validate_announcement_attempt(ann)?;
        if !matches!(ann.phase, Phase::Prepare | Phase::Aligned) {
            return Ok(());
        }
        // An assignment certificate is the barrier layer's cluster-runtime marker. Embedded and
        // single-node coordinators can be built with the cluster feature enabled, but do not have
        // a remote leader lease and must retain their local KV path.
        if ann.assignment_fence.is_none() {
            return Ok(());
        }
        let proof = ann.leader_proof.as_ref().ok_or_else(|| {
            format!(
                "clustered {:?} for checkpoint {}/{} is missing a durable leader proof",
                ann.phase, ann.epoch, ann.checkpoint_id
            )
        })?;
        self.validate_announcement_leader(ann, proof).await
    }

    #[cfg(feature = "cluster")]
    pub(super) fn require_exact_local_leader_proof(
        &self,
        ann: &BarrierAnnouncement,
    ) -> Result<(), String> {
        let expected = ann
            .leader_proof
            .as_ref()
            .ok_or_else(|| "assignment-certified barrier has no exact leader proof".to_string())?;
        let provider = self
            .local_leader_proof
            .lock()
            .clone()
            .ok_or_else(|| "local leader proof provider is not installed".to_string())?;
        if provider().as_ref() != Some(expected) {
            return Err(
                "assignment-certified barrier sender no longer owns its exact leader proof".into(),
            );
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(super) async fn validate_announcement_leader(
        &self,
        ann: &BarrierAnnouncement,
        proof: &crate::checkpoint::LeaderProof,
    ) -> Result<(), String> {
        let local_proof = self
            .local_leader_proof
            .lock()
            .clone()
            .and_then(|provider| provider());
        if local_proof.as_ref() == Some(proof) {
            return Ok(());
        }
        let store = self
            .leader_lease_store
            .lock()
            .clone()
            .ok_or_else(|| "durable leader lease store is not installed".to_string())?;
        let lease = store
            .load()
            .await
            .map_err(|error| format!("leader lease read failed: {error}"))?
            .ok_or_else(|| "no durable leader lease exists".to_string())?;
        if !lease.matches_proof(proof) {
            return Err(format!(
                "clustered {:?} for checkpoint {}/{} does not match the latest durable leader lease",
                ann.phase, ann.epoch, ann.checkpoint_id
            ));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(in crate::cluster::control) async fn validate_checkpoint_prepare(
        &self,
        announcement: &BarrierAnnouncement,
    ) -> Result<(), String> {
        validate_announcement_attempt(announcement)?;
        if announcement.phase != Phase::Prepare {
            return Err("checkpoint Prepare validation received a different barrier phase".into());
        }
        let proof = announcement.leader_proof.as_ref().ok_or_else(|| {
            format!(
                "clustered Prepare for checkpoint {}/{} is missing a durable leader proof",
                announcement.epoch, announcement.checkpoint_id
            )
        })?;
        self.validate_announcement_leader(announcement, proof).await
    }

    /// Configure membership used to target active barrier peers.
    /// Gossip election is not a barrier authority boundary.
    #[cfg(feature = "cluster")]
    pub fn set_leader_election(
        &mut self,
        instance_id: NodeId,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
        leader_eligible: Arc<AtomicBool>,
    ) {
        *self.leader_election.lock() = Some((instance_id, members_rx, leader_eligible));
    }

    #[cfg(feature = "cluster")]
    pub(super) fn direct_prepare_received_at(
        &self,
        identity: BarrierIdentity,
    ) -> Option<std::time::Instant> {
        self.grpc.lock().as_ref().and_then(|state| {
            state
                .prepare_acks
                .lock()
                .received_at
                .get(&identity)
                .copied()
        })
    }

    /// Preserve the first local clock for an exact Prepare across retries and across direct or
    /// gossip delivery. A direct accepted receipt wins when it predates local gossip observation.
    #[cfg(feature = "cluster")]
    pub(in crate::cluster::control) fn prepare_received_at_or_insert(
        &self,
        prepare: &BarrierAnnouncement,
        observed_at: std::time::Instant,
    ) -> Option<std::time::Instant> {
        if prepare.phase != Phase::Prepare {
            return None;
        }
        let identity = BarrierIdentity::from_announcement(prepare);
        let candidate = self
            .direct_prepare_received_at(identity)
            .map_or(observed_at, |received_at| received_at.min(observed_at));
        let mut observations = self.prepare_observed_at.lock();
        if !observations.contains_key(&identity) {
            while observations.len() >= MAX_RETAINED_BARRIER_IDENTITIES {
                let Some(oldest) = observations
                    .iter()
                    .min_by_key(|(_, observed_at)| **observed_at)
                    .map(|(identity, _)| *identity)
                else {
                    break;
                };
                observations.remove(&oldest);
            }
        }
        let retained = *observations
            .entry(identity)
            .and_modify(|retained| *retained = (*retained).min(candidate))
            .or_insert(candidate);
        Some(retained)
    }

    /// Local monotonic receipt or first-observation time for this exact Prepare.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn prepare_received_at(&self, prepare: &BarrierAnnouncement) -> Option<std::time::Instant> {
        if prepare.phase != Phase::Prepare {
            return None;
        }
        let identity = BarrierIdentity::from_announcement(prepare);
        match (
            self.direct_prepare_received_at(identity),
            self.prepare_observed_at.lock().get(&identity).copied(),
        ) {
            (Some(direct), Some(observed)) => Some(direct.min(observed)),
            (Some(received), None) | (None, Some(received)) => Some(received),
            (None, None) => None,
        }
    }

    /// Install one direct Prepare receipt through the real relay for deterministic controller
    /// observation tests. Production direct delivery records the same clock before enqueueing.
    ///
    /// # Errors
    /// Rejects malformed/non-Prepare input, a missing or closed relay, or a relay that does not
    /// publish the injected value.
    #[cfg(all(test, feature = "cluster"))]
    pub(in crate::cluster::control) async fn inject_direct_prepare_observation_for_test(
        &self,
        prepare: BarrierAnnouncement,
        received_at: std::time::Instant,
    ) -> Result<(), String> {
        validate_announcement_attempt(&prepare)?;
        if prepare.phase != Phase::Prepare {
            return Err("direct Prepare test observation requires Prepare phase".into());
        }
        let state = self.grpc.lock().clone().ok_or_else(|| {
            "direct Prepare test observation requires a started server".to_string()
        })?;
        let identity = BarrierIdentity::from_announcement(&prepare);
        state
            .prepare_acks
            .lock()
            .received_at
            .entry(identity)
            .or_insert(received_at);
        if state.incoming_tx.send(prepare.clone()).await.is_err() {
            return Err("direct Prepare test relay is closed".into());
        }
        for _ in 0..1_024 {
            if state.latest_rx.borrow().as_ref() == Some(&prepare) {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err("direct Prepare test observation was not relayed".into())
    }

    /// Bind and run the follower's direct gRPC barrier sync server.
    ///
    /// # Errors
    /// Returns an error string on bind or socket address retrieval failures.
    #[cfg(feature = "cluster")]
    pub async fn start_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
    ) -> Result<std::net::SocketAddr, String> {
        use barrier_v1::barrier_sync_server::BarrierSyncServer;
        use std::net::TcpListener;
        use tonic::transport::Server;

        self.require_live_bound_process_lease()?;
        let advertised_process = self.claim_endpoint_process();
        let listener = TcpListener::bind(bind_addr).map_err(|e| e.to_string())?;
        let local_addr = listener.local_addr().map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let tokio_listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| e.to_string())?;
        let advertise_addr = if let Some(ref host) = advertise_host {
            format!("{host}:{}", local_addr.port())
        } else if local_addr.ip().is_unspecified() {
            let hostname = gethostname::gethostname();
            let hostname = hostname.to_string_lossy();
            if hostname.is_empty() {
                local_addr.to_string()
            } else {
                format!("{hostname}:{}", local_addr.port())
            }
        } else {
            local_addr.to_string()
        };
        let advertisement = encode_barrier_endpoint(&advertise_addr, advertised_process)?;

        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(128);
        let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
        let clients = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
        let local_process = Arc::clone(&self.local_process);

        let server_impl = GrpcBarrierServer {
            incoming_tx: incoming_tx.clone(),
            prepare_acks: Arc::clone(&prepare_acks),
            leader_lease_store: Arc::clone(&self.leader_lease_store),
            local_leader_proof: Arc::clone(&self.local_leader_proof),
            local_process: Arc::clone(&local_process),
            process_lease_deadline: Arc::clone(&self.process_lease_deadline),
        };

        // Apply TLS synchronously so a bad cert fails start_server (before
        // publishing BARRIER_ADDR_KEY) rather than silently never serving.
        let mut builder = Server::builder();
        if let Some(tls) = super::super::super::tls::server_tls() {
            builder = builder
                .tls_config(tls.clone())
                .map_err(|e| format!("cluster control-plane TLS config: {e}"))?;
        }
        let router = builder.add_service(BarrierSyncServer::new(server_impl));
        let server_task = tokio::spawn(async move {
            let incoming_stream = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
            let _ = router.serve_with_incoming(incoming_stream).await;
        });

        // Relay every gRPC-delivered announcement into a relation-validated
        // watch in arrival order. Observation is then non-destructive,
        // so the pipeline's resume gate and the background durable
        // tail can watch concurrently (matching the gossip-KV
        // fallback's read-latest semantics).
        let (latest_tx, latest_rx) = watch::channel::<Option<BarrierAnnouncement>>(None);
        let merge_error = Arc::new(parking_lot::Mutex::new(None));
        let relay_merge_error = Arc::clone(&merge_error);
        let relay_task = tokio::spawn(async move {
            while let Ok(ann) = incoming_rx.recv().await {
                let merged = match latest_tx.borrow().clone() {
                    Some(current) => merge_direct_announcement(current, ann),
                    None => Ok(ann),
                };
                match merged {
                    Ok(merged) => {
                        let changed = latest_tx.borrow().as_ref() != Some(&merged);
                        if changed {
                            let _ = latest_tx.send(Some(merged));
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "rejecting conflicting direct barrier history");
                        let mut retained = relay_merge_error.lock();
                        if retained.is_none() {
                            *retained = Some(error);
                        }
                    }
                }
            }
        });

        let grpc_state = Arc::new(GrpcState {
            latest_rx,
            incoming_tx,
            merge_error,
            prepare_acks,
            prepare_fanout: parking_lot::Mutex::new(None),
            clients,
            server_handle: Arc::new(parking_lot::Mutex::new(Some(server_task))),
            relay_handle: Arc::new(parking_lot::Mutex::new(Some(relay_task))),
            local_process: Arc::clone(&local_process),
        });

        if let Err(error) = self.require_live_bound_process_lease() {
            abort_grpc_tasks(&grpc_state);
            return Err(error);
        }
        if let Err(error) = self.kv.write_checked(BARRIER_ADDR_KEY, advertisement).await {
            abort_grpc_tasks(&grpc_state);
            return Err(format!(
                "publish cluster control endpoint advertisement: {error}"
            ));
        }

        *self.grpc.lock() = Some(grpc_state);

        Ok(local_addr)
    }

    /// Ask one exact remote process to confirm a proof already read from durable authority.
    ///
    /// The response echoes only a fresh challenge id. It never returns a process-local or durable
    /// fencing token.
    ///
    /// # Errors
    /// Fails when the proof, peer address, RPC, acknowledgement, or deadline is invalid.
    #[cfg(feature = "cluster")]
    pub async fn confirm_remote_leader_proof(
        &self,
        proof: &super::super::super::LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        if !proof.is_canonical() {
            return Err("remote leader proof challenge is not canonical".into());
        }
        let peer = NodeId(proof.owner.node_id);
        let state = self
            .grpc
            .lock()
            .clone()
            .ok_or_else(|| "cluster control RPC server is not started".to_string())?;
        let clients = Arc::clone(&state.clients);
        let request_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let challenge = uuid::Uuid::new_v4();
        let expected_process = Some(ExpectedBarrierProcess::exact(&proof.owner));
        let result = tokio::time::timeout_at(deadline, async {
            let mut client =
                match get_barrier_client(peer, expected_process, &clients, &self.kv).await {
                    Ok(Some(client)) => client,
                    Ok(None) => {
                        return Err(format!(
                            "cluster control address for peer {} is unavailable",
                            peer.0
                        ));
                    }
                    Err(BarrierClientResolutionError::ProcessMismatch) => return Ok(false),
                    Err(BarrierClientResolutionError::Invalid(error)) => return Err(error),
                };
            let mut request = tonic::Request::new(barrier_v1::LeaderProofChallenge {
                expected_proof: leader_proof_to_wire(Some(proof)),
                challenge_id: challenge.as_bytes().to_vec(),
            });
            request.set_timeout(request_timeout);
            match client.confirm_leader_proof(request).await {
                Ok(response) => {
                    let acknowledged = response.into_inner().challenge_id;
                    if !leader_proof_ack_matches(challenge, &acknowledged) {
                        return Err("remote leader proof acknowledgement challenge mismatch".into());
                    }
                    Ok(true)
                }
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    // The stable node id may now advertise a replacement process. Do not pin
                    // subsequent proof attempts to a still-responsive channel for the old boot.
                    evict_barrier_client(&clients, peer, expected_process);
                    Ok(false)
                }
                Err(status) => Err(status.to_string()),
            }
        })
        .await;
        match result {
            Ok(Ok(confirmed)) => Ok(confirmed),
            Ok(Err(error)) => {
                evict_barrier_client(&clients, peer, expected_process);
                Err(error)
            }
            Err(_) => {
                evict_barrier_client(&clients, peer, expected_process);
                Err(format!(
                    "remote leader proof request for peer {} timed out",
                    peer.0
                ))
            }
        }
    }
}
