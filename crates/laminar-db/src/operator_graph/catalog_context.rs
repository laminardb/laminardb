#[cfg(feature = "cluster")]
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use super::OperatorGraph;
use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::operator::sql_query::SqlQueryOperator;

impl OperatorGraph {
    /// Register the static reference tables available to enrichment operators.
    pub fn set_reference_tables(&mut self, tables: FxHashSet<String>) {
        self.reference_tables = tables;
    }

    /// Seed changelog producers before operators are built so admission is build-order independent.
    pub fn set_changelog_tables(&mut self, tables: FxHashSet<String>) {
        self.changelog_tables = tables;
    }

    /// Install the complete startup-certified mutable interval topology before graph construction.
    pub(crate) fn set_ordered_interval_joins(
        &mut self,
        joins: FxHashMap<String, [crate::operator::interval_join_input::BoundedJoinInputMode; 2]>,
    ) {
        if !self.nodes.is_empty() {
            self.build_errors.push(DbError::Config(
                "ordered interval topology must be installed before graph operators".into(),
            ));
            return;
        }
        self.ordered_interval_joins = joins;
    }

    /// Install the startup-bound subscription proofs before their final operators are built.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_subscription_certificates(
        &mut self,
        certificates: FxHashMap<
            String,
            Arc<laminar_core::checkpoint::OutputDistributionCertificate>,
        >,
    ) {
        if !self.nodes.is_empty() {
            self.build_errors.push(DbError::Config(
                "subscription certificates must be installed before graph operators".into(),
            ));
            return;
        }
        self.subscription_certificates = certificates;
    }

    #[cfg(feature = "cluster")]
    pub(super) fn attach_sql_query_cluster_context(
        &self,
        name: &str,
        operator: &mut SqlQueryOperator,
    ) -> Result<(), DbError> {
        if let Some(config) = &self.cluster_shuffle {
            debug_assert_eq!(
                config.registry.vnode_count(),
                u32::from(self.key_group_count)
            );
            operator.attach_cluster_shuffle(config.clone());
        }
        if let Some(certificate) = self.subscription_certificates.get(name) {
            operator.attach_subscription_certificate(Arc::clone(certificate))?;
        }
        Ok(())
    }
}
