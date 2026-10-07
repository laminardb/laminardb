//! Process bindings are sealed as typed stream DDL; code is supplied by the deployment.

use super::ProcessFunctionRegistration;
use crate::db::DbState;
use crate::{DbError, DdlInfo, ExecuteResult, LaminarDB};

impl ProcessFunctionRegistration {
    fn catalog_ddl(&self) -> Result<String, DbError> {
        let manifest = String::from_utf8(self.descriptor.to_manifest_json()?)
            .map_err(|error| DbError::InvalidOperation(error.to_string()))?;
        Ok(format!(
            "CREATE STREAM {} AS SELECT * FROM laminar_process('{}', '{}')",
            self.output_name,
            self.source_name,
            manifest.replace('\'', "''"),
        ))
    }
}

impl LaminarDB {
    /// Return the canonical invocation DDL for a registered cluster process binding.
    /// Include this statement after its source and before its consumers in
    /// [`Self::execute_cluster_bootstrap_batch`]. Every owner must register the same immutable
    /// implementation before bootstrapping or replaying that catalog. SQL never loads code.
    ///
    /// # Errors
    /// Rejects an unknown binding or an incompatible manifest.
    #[cfg(feature = "cluster")]
    pub fn process_function_bootstrap_sql(&self, output_name: &str) -> Result<String, DbError> {
        self.connector_manager
            .lock()
            .process_functions()
            .get(output_name)
            .ok_or_else(|| DbError::InvalidOperation("process binding does not exist".into()))?
            .catalog_ddl()
    }

    pub(crate) fn create_bound_process_stream(
        &self,
        sql: &str,
        output_name: &str,
    ) -> Result<Option<ExecuteResult>, DbError> {
        let registration = self
            .connector_manager
            .lock()
            .process_functions()
            .get(output_name)
            .cloned();
        let Some(registration) = registration else {
            return Ok(None);
        };
        if !self.is_cluster_runtime() || DbState::load(&self.state) != DbState::Created {
            return Err(DbError::Unsupported(
                "process invocation DDL requires cluster startup bootstrap".into(),
            ));
        }
        if crate::pipeline_identity::canonical_sql(sql)
            != crate::pipeline_identity::canonical_sql(&registration.catalog_ddl()?)
        {
            return Err(DbError::Unsupported(
                "process stream DDL differs from its immutable deployment binding".into(),
            ));
        }
        self.install_process_function(&registration)?;
        Ok(Some(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE STREAM".into(),
            object_name: output_name.into(),
            #[cfg(feature = "cluster")]
            topology_operation: None,
            applied: true,
        })))
    }
}

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use super::*;
    use crate::process_function::tests::{descriptor, AccountActivity};
    use laminar_connectors::connector::DeliveryGuarantee;
    use laminar_core::cluster::control::{ClusterController, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use std::sync::Arc;

    async fn database(delivery: DeliveryGuarantee) -> Arc<LaminarDB> {
        let (_, membership) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(
            NodeId(7),
            Arc::new(InMemoryKv::new(NodeId(7))),
            None,
            membership,
        ));
        controller
            .set_process_lease_deadline(Arc::new(
                laminar_core::cluster::control::LeaseDeadline::live_for(
                    std::time::Duration::from_secs(30),
                ),
            ))
            .unwrap();
        LaminarDB::builder()
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .delivery_guarantee(delivery)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cluster_process_registration_requires_at_least_once() {
        let db = database(DeliveryGuarantee::ExactlyOnce).await;
        assert!(db
            .register_native_process_function(
                "activity",
                "events",
                descriptor(),
                Arc::new(AccountActivity)
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("delivery"));
        assert!(db.process_functions().is_empty());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn process_catalog_binding_rejects_reordering_and_package_drift() {
        let db = database(DeliveryGuarantee::AtLeastOnce).await;
        let mut binding = descriptor();
        binding.function_id = "account'activity".into();
        db.register_native_process_function(
            "activity",
            "events",
            binding,
            Arc::new(AccountActivity),
        )
        .await
        .unwrap();
        let ddl = db.process_function_bootstrap_sql("activity").unwrap();
        assert_eq!(laminar_sql::parse_streaming_sql(&ddl).unwrap().len(), 1);
        for changed in [
            format!("{ddl} WHERE amount > 0"),
            format!("{ddl} LIMIT 1"),
            ddl.replace("account''activity", "another_activity"),
        ] {
            assert!(db
                .create_bound_process_stream(&changed, "activity")
                .unwrap_err()
                .to_string()
                .contains("immutable deployment binding"));
            assert!(db.catalog.get_stream_entry("activity").is_none());
        }
        assert!(db
            .register_native_process_function(
                "activity",
                "events",
                descriptor(),
                Arc::new(AccountActivity)
            )
            .await
            .is_err());
        assert!(db.process_function_bootstrap_sql("unknown").is_err());
        assert!(db
            .validate_cluster_topology_change(
                laminar_core::cluster::control::TopologyVersion::LEGACY_BASELINE,
                &["DROP STREAM activity".into()],
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("process bindings are immutable"));
        db.shutdown().await.unwrap();
    }
}
