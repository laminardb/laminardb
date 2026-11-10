use super::ConnectorPipelineCallback;
use crate::pipeline::callback::ExternalOutputPressure;

impl ConnectorPipelineCallback {
    pub(super) fn output_pressure(&self) -> ExternalOutputPressure {
        #[cfg(feature = "cluster")]
        if self.in_cluster() {
            return self.cluster_subscription_output.output_pressure();
        }
        ExternalOutputPressure::Normal
    }
}
