use super::ConnectorPipelineCallback;
use crate::pipeline::callback::ExternalOutputPressure;

impl ConnectorPipelineCallback {
    pub(super) fn graph_backpressured(&self) -> bool {
        let backpressured = self.graph.input_buf_pressure() > 0.8;
        if backpressured {
            self.prom.cycles_backpressured.inc();
        }
        backpressured
    }

    pub(super) fn output_pressure(&self) -> ExternalOutputPressure {
        #[cfg(feature = "cluster")]
        if self.in_cluster() {
            return self.cluster_subscription_output.output_pressure();
        }
        ExternalOutputPressure::Normal
    }
}
