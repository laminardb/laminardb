use super::ConnectorPipelineCallback;

impl ConnectorPipelineCallback {
    pub(super) fn graph_backpressured(&self) -> bool {
        let backpressured = self.graph.input_buf_pressure() > 0.8;
        if backpressured {
            self.prom.cycles_backpressured.inc();
        }
        backpressured
    }

    pub(super) fn output_commit_backpressured(&self) -> bool {
        #[cfg(feature = "cluster")]
        if self.in_cluster() {
            return self.cluster_subscription_output.commit_backpressured();
        }
        false
    }
}
