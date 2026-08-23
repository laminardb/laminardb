use super::OperatorGraph;

impl OperatorGraph {
    pub(crate) fn take_prepared_subscription_outputs(
        &mut self,
    ) -> Vec<crate::subscription::PreparedSubscriptionOutput> {
        if self.subscription_certificates.is_empty() {
            return Vec::new();
        }
        self.nodes
            .iter_mut()
            .filter(|node| !node.removed)
            .filter_map(|node| node.operator.take_prepared_subscription_output())
            .collect()
    }

    pub(crate) fn commit_prepared_subscription_outputs(&mut self) {
        if self.subscription_certificates.is_empty() {
            return;
        }
        for node in self.nodes.iter_mut().filter(|node| !node.removed) {
            node.operator.commit_prepared_subscription_output();
        }
    }

    pub(crate) fn abort_prepared_subscription_outputs(&mut self) {
        if self.subscription_certificates.is_empty() {
            return;
        }
        for node in self.nodes.iter_mut().filter(|node| !node.removed) {
            node.operator.abort_prepared_subscription_output();
        }
    }
}
