#[cfg(feature = "cluster")]
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use super::OperatorGraph;
use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::operator::sql_query::SqlQueryOperator;

impl OperatorGraph {
    pub(super) fn prepare_query_inputs(
        &mut self,
        name: &str,
        tables: &mut FxHashSet<String>,
        ordered_join: bool,
        lookup: Option<&crate::operator::lookup_enrich::LookupEnrichConfig>,
        changelog: Option<&crate::sql_analysis::ChangelogEnrichConfig>,
    ) -> Option<usize> {
        // Reference/lookup dimensions come from their providers, never from a stream port.
        tables.retain(|table| !self.reference_tables.contains(table));
        if let Some(config) = lookup {
            tables.remove(&config.table_name);
        }
        if let Some(config) = changelog {
            tables.retain(|table| table == &config.changelog_table);
        }
        let ports = if ordered_join { 2 } else { tables.len().max(1) };
        if ports > usize::from(u8::MAX) + 1 {
            self.build_errors.push(DbError::InvalidOperation(format!(
                "query '{name}' exceeds the 256 graph input-port limit"
            )));
            return None;
        }
        Some(ports)
    }

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
    pub(crate) fn subscription_certificates(
        &self,
    ) -> Vec<Arc<laminar_core::checkpoint::OutputDistributionCertificate>> {
        let mut certificates = self
            .subscription_certificates
            .values()
            .cloned()
            .collect::<Vec<_>>();
        certificates.sort_unstable_by(|left, right| left.stream_id.cmp(&right.stream_id));
        certificates
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
