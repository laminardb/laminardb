//! Requests for a checkpoint: a recent read's request to start the next periodic checkpoint
//! early, and the route to the cluster leader, which alone starts checkpoints.

use super::LaminarDB;

/// Bound on one forwarded request; a lost request leaves the read waiting for the cadence.
#[cfg(feature = "cluster")]
const FORWARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

impl LaminarDB {
    /// Ask for the checkpoint that commits `epoch` early, for a recent read about to wait for it.
    /// A cluster follower forwards the first request for each epoch to the leader.
    pub(super) fn expedite_checkpoint(&self, epoch: u64) {
        #[cfg(not(feature = "cluster"))]
        let _ = epoch;
        let demand = self.mv_store.read().checkpoint_demand();
        #[cfg(feature = "cluster")]
        if self.is_cluster_runtime() {
            match self.cluster_leader_http_address() {
                Ok(None) => {}
                Ok(Some(leader)) => {
                    if demand.first_to_forward(epoch) {
                        self.forward_expedite(leader);
                    }
                    return;
                }
                Err(reason) => {
                    tracing::debug!(%reason, "a recent read cannot ask the cluster leader for a checkpoint");
                    return;
                }
            }
        }
        demand.request();
    }

    /// Start the next periodic checkpoint early, as a recent read waiting on another cluster
    /// node asked. Only the leader starts checkpoints, and only when
    /// [`StreamCheckpointConfig::recent_read_interval_ms`] is set.
    ///
    /// [`StreamCheckpointConfig::recent_read_interval_ms`]:
    /// laminar_core::streaming::StreamCheckpointConfig::recent_read_interval_ms
    #[doc(hidden)]
    pub fn expedite_forwarded_checkpoint(&self) {
        self.mv_store.read().checkpoint_demand().request();
    }

    #[cfg(feature = "cluster")]
    fn forward_expedite(&self, leader: String) {
        let token = self.config.http_auth_token.clone();
        tokio::spawn(async move {
            let client = match reqwest::Client::builder().timeout(FORWARD_TIMEOUT).build() {
                Ok(client) => client,
                Err(error) => {
                    tracing::debug!(%error, "checkpoint request client failed to build");
                    return;
                }
            };
            let mut request = client.post(format!("http://{leader}/api/v1/checkpoint/expedite"));
            if let Some(token) = &token {
                request = request.bearer_auth(token.expose());
            }
            if let Err(error) = request
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
            {
                tracing::debug!(%error, %leader, "the cluster leader did not take a recent read's checkpoint request");
            }
        });
    }

    /// The cluster leader's HTTP address, or `None` when this node leads.
    #[cfg(feature = "cluster")]
    pub(super) fn cluster_leader_http_address(&self) -> Result<Option<String>, String> {
        let controller = self.cluster_controller.lock();
        let controller = controller
            .as_ref()
            .ok_or("cluster runtime has no cluster controller")?;
        if controller.is_leader() {
            return Ok(None);
        }
        let leader = controller
            .current_leader()
            .ok_or("cluster leader is unresolved")?;
        let members = controller.members_watch();
        let members = members.borrow();
        let address = members
            .iter()
            .find(|member| member.id == leader)
            .map(|member| member.rpc_address.trim())
            .filter(|address| !address.is_empty())
            .ok_or_else(|| format!("RPC address for cluster leader {leader} is unresolved"))?;
        Ok(Some(address.to_owned()))
    }
}
