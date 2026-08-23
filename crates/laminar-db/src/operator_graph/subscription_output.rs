use super::OperatorGraph;

impl OperatorGraph {
    pub(crate) fn capture_subscription_frontiers(
        &self,
    ) -> Result<Vec<crate::subscription::CertifiedSubscriptionFrontiers>, crate::error::DbError>
    {
        if self.subscription_certificates.is_empty() {
            return Ok(Vec::new());
        }
        let mut captures = self
            .nodes
            .iter()
            .filter(|node| !node.removed)
            .filter_map(|node| node.operator.certified_subscription_frontiers().transpose())
            .collect::<Result<Vec<_>, _>>()?;
        captures.sort_unstable_by(|left, right| {
            left.certificate.stream_id.cmp(&right.certificate.stream_id)
        });
        let exact = captures.len() == self.subscription_certificates.len()
            && captures.iter().all(|capture| {
                self.subscription_certificates
                    .get(&capture.certificate.stream_id)
                    .is_some_and(|expected| expected.as_ref() == capture.certificate.as_ref())
            });
        if !exact {
            return Err(crate::error::DbError::Checkpoint(
                "subscription frontier capture does not match the certified stream roster".into(),
            ));
        }
        Ok(captures)
    }

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
