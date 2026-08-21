//! Controller construction and committed-progress publication.

use super::*;

impl ClusterController {
    /// Wrap the given primitives.
    #[must_use]
    pub fn new(
        instance_id: NodeId,
        kv: Arc<dyn ClusterKv>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        Self::new_with_recovery_kv(instance_id, Arc::clone(&kv), kv, snapshot, members_rx)
    }

    /// Wrap the given primitives with a separate durable recovery authority.
    ///
    /// The barrier/discovery KV remains the low-latency path. `recovery_kv` owns recovery
    /// incarnations, generations, phase announcements, and acknowledgements.
    #[must_use]
    pub fn new_with_recovery_kv(
        instance_id: NodeId,
        kv: Arc<dyn ClusterKv>,
        recovery_kv: Arc<dyn ClusterKv>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        Self::new_with_recovery_incarnation(
            instance_id,
            kv,
            recovery_kv,
            snapshot,
            members_rx,
            Uuid::new_v4(),
        )
    }

    /// Wrap the control primitives with a caller-generated boot identity.
    #[must_use]
    pub fn new_with_recovery_incarnation(
        instance_id: NodeId,
        kv: Arc<dyn ClusterKv>,
        recovery_kv: Arc<dyn ClusterKv>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
        recovery_incarnation: Uuid,
    ) -> Self {
        let leader_eligible = Arc::new(AtomicBool::new(true));
        let mut barrier = BarrierCoordinator::new(Arc::clone(&kv));
        #[cfg(feature = "cluster")]
        barrier.set_leader_election(
            instance_id,
            members_rx.clone(),
            Arc::clone(&leader_eligible),
        );
        let controller = Self {
            instance_id,
            barrier,
            kv,
            recovery_kv,
            snapshot,
            members_rx,
            // A new leader must not checkpoint until it proves exact assignment convergence.
            checkpoint_assignment_fence: watch::channel(None).0,
            checkpoint_drain_transition: watch::channel(None).0,
            process_authority_transition: parking_lot::Mutex::new(()),
            recovery_incarnation,
            recovery_process_term: AtomicU64::new(0),
            recovery_fault_request_sequence: AtomicU64::new(1),
            cluster_min_watermark: Arc::new(AtomicI64::new(i64::MIN)),
            committed_source_watermarks: parking_lot::RwLock::new(Arc::new(
                rustc_hash::FxHashMap::default(),
            )),
            committed_watermark_publication: parking_lot::Mutex::new(()),
            draining: Arc::new(AtomicBool::new(false)),
            recovering: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicBool::new(true)),
            process_lease_live: Arc::new(AtomicBool::new(true)),
            process_lease_deadline: std::sync::OnceLock::new(),
            process_lease_authority: std::sync::OnceLock::new(),
            leader_eligible,
            leader_candidacy: watch::channel(super::super::LeaderCandidacy::initial(false)).0,
            leadership_participants: parking_lot::RwLock::new(None),
            recovery_writes: tokio::sync::Mutex::new(()),
            pending_release_fault_audit: tokio::sync::Mutex::new(None),
            unresponsive: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
            self_locality: parking_lot::RwLock::new(Locality::default()),
            #[cfg(feature = "cluster")]
            leader_lease: std::sync::OnceLock::new(),
        };
        controller.notify_leader_eligibility_change();
        controller
    }

    /// Install the durable authority used to validate clustered checkpoint barriers.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_store(&self, store: Arc<super::super::LeaderLeaseStore>) {
        self.barrier.set_leader_lease_store(store);
    }

    /// Serve only leader proofs that remain live on this process's monotonic lease gate.
    #[cfg(feature = "cluster")]
    pub fn install_local_leader_proof_provider(self: &Arc<Self>) {
        let controller = Arc::downgrade(self);
        self.barrier
            .set_local_leader_proof_provider(Arc::new(move || {
                controller
                    .upgrade()
                    .and_then(|controller| controller.capture_leader_proof())
            }));
    }

    /// Exact durable authority installed for this cluster controller.
    ///
    /// Cluster checkpoint code must use this handle rather than standalone outcome keys.
    ///
    /// # Errors
    /// Returns [`super::super::ClusterCheckpointAuthorityError::NotConfigured`] when no authority exists.
    #[cfg(feature = "cluster")]
    pub fn checkpoint_authority(
        &self,
    ) -> Result<Arc<super::super::LeaderLeaseStore>, super::super::ClusterCheckpointAuthorityError>
    {
        self.barrier.checkpoint_authority()
    }

    /// Latest recovery-safe cluster watermark installed from an immutable Commit outcome or a
    /// validated committed checkpoint.
    #[must_use]
    pub fn cluster_min_watermark(&self) -> Option<i64> {
        let v = self.cluster_min_watermark.load(Ordering::Acquire);
        if v == i64::MIN {
            None
        } else {
            Some(v)
        }
    }

    /// Mirror the leader's computed cluster-min watermark into the atomic so its own operators
    /// match followers. Monotonic — never lowers the published value.
    pub fn publish_cluster_min_watermark(&self, wm: i64) {
        let _publication = self.committed_watermark_publication.lock();
        self.publish_cluster_min_watermark_locked(wm);
    }

    pub(super) fn publish_cluster_min_watermark_locked(&self, wm: i64) {
        let mut cur = self.cluster_min_watermark.load(Ordering::Acquire);
        while wm > cur {
            match self.cluster_min_watermark.compare_exchange(
                cur,
                wm,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Pin one internally consistent source-frontier snapshot for a compute cycle.
    #[must_use]
    pub fn committed_source_watermarks_snapshot(&self) -> Arc<rustc_hash::FxHashMap<String, i64>> {
        let snapshot = self.committed_source_watermarks.read();
        Arc::clone(&snapshot)
    }

    /// Install both scalar and source-keyed frontiers from one immutable committed channel cut.
    ///
    /// Publication is monotonic per source, matching [`Self::publish_cluster_min_watermark`]. A
    /// source withheld by an active uninitialized channel remains unpublished until a later
    /// committed cut initializes it.
    ///
    /// # Errors
    /// Returns an error when the committed channel cut contains an invalid watermark sentinel.
    pub fn publish_committed_channel_progress(
        &self,
        channels: &[crate::checkpoint::ChannelProgress],
    ) -> Result<(), String> {
        let source_watermarks = crate::checkpoint::channel_progress_frontiers_by_source(channels)?
            .into_iter()
            .filter_map(|(source, frontier)| frontier.map(|frontier| (source.to_owned(), frontier)))
            .collect::<std::collections::BTreeMap<_, _>>();
        self.publish_committed_checkpoint_progress(channels, &source_watermarks)
    }

    /// Install scalar and source-keyed frontiers from one committed checkpoint index.
    ///
    /// The explicit source map retains decisions for a source whose physical channel inventory is
    /// empty in this cut. Publication remains monotonic so an older observer cannot regress a
    /// newer installed decision.
    ///
    /// # Errors
    /// Returns an error when channel progress or an explicit source watermark is invalid or the
    /// explicit map disagrees with an initialized channel cut.
    pub fn publish_committed_checkpoint_progress(
        &self,
        channels: &[crate::checkpoint::ChannelProgress],
        source_watermarks: &std::collections::BTreeMap<String, i64>,
    ) -> Result<(), String> {
        let cluster_min = crate::checkpoint::channel_progress_frontier(channels)?;
        let current = crate::checkpoint::channel_progress_frontiers_by_source(channels)?;
        if source_watermarks
            .values()
            .any(|watermark| *watermark == i64::MIN)
        {
            return Err("committed source watermark uses the reserved uninitialized value".into());
        }
        for (source, frontier) in current {
            let Some(frontier) = frontier else {
                continue;
            };
            if source_watermarks.get(source) != Some(&frontier) {
                return Err(format!(
                    "committed source watermark '{source}' disagrees with channel progress"
                ));
            }
        }
        let _publication = self.committed_watermark_publication.lock();
        {
            let mut published = self.committed_source_watermarks.write();
            let published = Arc::make_mut(&mut *published);
            for (source_name, frontier) in source_watermarks {
                published
                    .entry(source_name.clone())
                    .and_modify(|current| *current = (*current).max(*frontier))
                    .or_insert(*frontier);
            }
        }
        if let Some(cluster_min) = cluster_min {
            self.publish_cluster_min_watermark_locked(cluster_min);
        }
        Ok(())
    }

    /// Replace the process-local committed frontier snapshot with one recovered exact cut.
    ///
    /// Unlike live publication, recovery may deliberately rewind to an older checkpoint or to
    /// genesis. The caller must hold the pipeline recovery/intake fence; this method serialises
    /// against any in-flight live publication and atomically establishes the new logical baseline
    /// before intake is reopened.
    ///
    /// # Errors
    /// Returns an error when channel progress or an explicit source watermark is invalid or the
    /// explicit map disagrees with an initialized channel cut.
    pub fn replace_recovered_checkpoint_progress(
        &self,
        channels: &[crate::checkpoint::ChannelProgress],
        source_watermarks: &std::collections::BTreeMap<String, i64>,
    ) -> Result<(), String> {
        let cluster_min = crate::checkpoint::channel_progress_frontier(channels)?;
        let current = crate::checkpoint::channel_progress_frontiers_by_source(channels)?;
        if source_watermarks
            .values()
            .any(|watermark| *watermark == i64::MIN)
        {
            return Err("committed source watermark uses the reserved uninitialized value".into());
        }
        for (source, frontier) in current {
            let Some(frontier) = frontier else {
                continue;
            };
            if source_watermarks.get(source) != Some(&frontier) {
                return Err(format!(
                    "committed source watermark '{source}' disagrees with channel progress"
                ));
            }
        }

        let replacement = source_watermarks
            .iter()
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect::<rustc_hash::FxHashMap<_, _>>();
        let _publication = self.committed_watermark_publication.lock();
        *self.committed_source_watermarks.write() = Arc::new(replacement);
        self.cluster_min_watermark
            .store(cluster_min.unwrap_or(i64::MIN), Ordering::Release);
        Ok(())
    }
}
