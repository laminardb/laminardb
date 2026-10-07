//! Resolve and admit a private catalog, seal it, then install its committed contracts.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::FutureExt;
use laminar_core::cluster::control::{CatalogManifest, CatalogObjectKind, LeaderProof};
use laminar_sql::parser::{parse_streaming_sql, StreamingStatement};

use super::{
    validate_cluster_catalog_create, CatalogBootstrapGuard, DbState, LaminarDB, CATALOG_BOOTSTRAP,
};
use crate::error::DbError;
use crate::handle::ExecuteResult;

type BootstrapEntry = (String, StreamingStatement, String, CatalogObjectKind);

impl LaminarDB {
    /// Resolve, validate and durably seal the complete startup catalog before activation.
    /// Existing sealed catalogs accept their unchanged original definitions without discovery.
    ///
    /// # Errors
    /// Rejects divergent definitions, unsupported plans, stale leader authority and failed sealing.
    /// Authorized external preparation may leave unused artifacts if publication fails.
    pub async fn execute_cluster_bootstrap_batch(
        &self,
        sql: &[String],
    ) -> Result<Vec<ExecuteResult>, DbError> {
        let _creation = self.schema_creation_lock.lock().await;
        {
            let _catalog = self.topology_ddl_lock.read().await;
            self.ensure_bootstrap_available()?;
        }
        self.connector_registry.freeze();
        let parsed = parse_bootstrap(self, sql)?;
        // No catalog lock spans discovery or object-store I/O. The creation gate serializes DDL.
        if let Some(manifest) = self.restore_catalog_from_manifest().boxed().await? {
            return self
                .validate_sealed_topology_bootstrap(&manifest, &parsed)
                .await;
        }
        let store = self.catalog_manifest_store.lock().clone().ok_or_else(|| {
            DbError::Pipeline("cluster catalog manifest store is not configured".into())
        })?;
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Pipeline(
                "[LDB-6043] cluster catalog bootstrap requires a cluster controller".into(),
            )
        })?;
        let proof = controller
            .capture_catalog_bootstrap_proof()
            .ok_or_else(|| {
                DbError::Pipeline(
                    "[LDB-6043] cluster catalog bootstrap requires the active durable leader lease"
                        .into(),
                )
            })?;
        self.recheck_unsealed_bootstrap(&proof)?;
        let candidate = self.isolated_topology_catalog()?;
        for (sql, statement, name, _) in &parsed {
            let result = CATALOG_BOOTSTRAP
                .scope((), candidate.execute_parsed_single(sql, statement).boxed())
                .await?;
            if !matches!(result, ExecuteResult::Ddl(ref info) if info.applied && info.object_name == *name)
            {
                return Err(DbError::Pipeline(format!(
                    "candidate definition '{name}' did not apply exactly once"
                )));
            }
            self.recheck_unsealed_bootstrap(&proof)?;
        }
        // Reuse the startup compiler, including exact delivery, topology and mutation admission.
        let complete_connector_graph = {
            let manager = candidate.connector_manager.lock();
            manager.sources().len() == candidate.catalog.list_sources().len()
                && manager
                    .sinks()
                    .values()
                    .all(|sink| sink.connector_type.is_some())
        };
        if complete_connector_graph {
            candidate.plan_topology_graph().await?;
        } else if candidate
            .connector_manager
            .lock()
            .sinks()
            .values()
            .any(|sink| sink.connector_type.is_some())
        {
            return Err(DbError::Unsupported("external sink preparation requires a complete admitted connector graph; catalog ingress sources cannot prove external delivery".into()));
        }
        self.recheck_unsealed_bootstrap(&proof)?;
        self.prepare_bootstrap_sinks(&candidate, &proof).await?;
        let manifest = CatalogManifest::new(candidate.catalog_manifest_inventory()?)
            .map_err(|error| DbError::Pipeline(format!("invalid resolved catalog: {error}")))?;
        #[cfg(test)]
        let gate = { self.catalog_seal_gate.lock().clone() };
        #[cfg(test)]
        if let Some((entered, release)) = gate {
            entered.notify_one();
            release.notified().await;
        }
        self.recheck_unsealed_bootstrap(&proof)?;
        store
            .seal(&manifest, &proof)
            .boxed()
            .await
            .map_err(|error| DbError::Pipeline(format!("catalog manifest seal failed: {error}")))?;
        self.recheck_unsealed_bootstrap(&proof)?;
        let _catalog = self.topology_ddl_lock.write().await;
        self.recheck_unsealed_bootstrap(&proof)?;
        self.install_bootstrap(&manifest, &parsed).await
    }

    fn ensure_bootstrap_available(&self) -> Result<(), DbError> {
        self.ensure_catalog_cleanup_unfenced("cluster catalog bootstrap")?;
        self.ensure_coordinated_recovery_mutation_unfenced("cluster catalog bootstrap")?;
        if self.shutdown.load(Ordering::Acquire) {
            return Err(DbError::Shutdown);
        }
        if DbState::load(&self.state) != DbState::Created {
            return Err(DbError::InvalidOperation(
                "cluster catalog bootstrap is only valid before pipeline startup".into(),
            ));
        }
        Ok(())
    }

    fn recheck_unsealed_bootstrap(&self, proof: &LeaderProof) -> Result<(), DbError> {
        self.ensure_bootstrap_available()?;
        self.validate_catalog_seal_authority(Some(proof))?;
        if !self.catalog_manifest_inventory()?.is_empty() {
            return Err(DbError::Pipeline(
                "cannot seal a new cluster catalog over uncommitted local topology".into(),
            ));
        }
        Ok(())
    }

    async fn prepare_bootstrap_sinks(
        &self,
        candidate: &LaminarDB,
        proof: &LeaderProof,
    ) -> Result<(), DbError> {
        let registrations = candidate.connector_manager.lock().sinks().clone();
        let mut names: Vec<_> = registrations.keys().collect();
        names.sort_unstable();
        for name in names {
            self.recheck_unsealed_bootstrap(proof)?;
            let registration = &registrations[name];
            let Some(resolved_binding) = &registration.schema_binding else {
                continue;
            };
            let mut binding = resolved_binding.clone();
            let mut config = crate::connector_manager::build_sink_config(
                registration,
                candidate.config.delivery_guarantee,
            )?;
            config.set(
                "_arrow_schema",
                crate::pipeline_callback::encode_arrow_schema(&Arc::new(binding.logical.clone())),
            );
            let mut sink = candidate.connector_registry.create_sink(&config, None)?;
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                sink.prepare_schema(&config, &mut binding),
            )
            .await
            .map_err(|_| {
                DbError::Connector(format!(
                    "sink '{name}' schema preparation exceeded 30 seconds"
                ))
            })??;
            self.recheck_unsealed_bootstrap(proof)?;
            laminar_connectors::schema::resolution::validate_prepared_writer(
                &config,
                resolved_binding,
                &binding,
            )
            .map_err(|error| {
                DbError::Connector(format!("sink '{name}' schema preparation: {error}"))
            })?;
            candidate
                .connector_manager
                .lock()
                .set_schema_binding(name, binding)?;
        }
        Ok(())
    }

    async fn install_bootstrap(
        &self,
        manifest: &CatalogManifest,
        parsed: &[BootstrapEntry],
    ) -> Result<Vec<ExecuteResult>, DbError> {
        let mut guard = CatalogBootstrapGuard {
            db: self,
            created: Vec::with_capacity(parsed.len()),
            sealed: false,
        };
        let mut results = Vec::with_capacity(parsed.len());
        for ((sql, statement, name, kind), entry) in parsed.iter().zip(&manifest.entries) {
            let result = CATALOG_BOOTSTRAP
                .scope(
                    (),
                    crate::ddl::schema_resolution::RESOLVED_SCHEMA.scope(
                        entry.schema_binding.clone(),
                        self.execute_parsed_single(sql, statement).boxed(),
                    ),
                )
                .await?;
            if !matches!(&result, ExecuteResult::Ddl(info) if info.applied && info.object_name == *name)
            {
                return Err(DbError::Pipeline(format!(
                    "cluster catalog create '{name}' did not apply exactly once"
                )));
            }
            guard.record(name.clone(), *kind);
            results.push(result);
        }
        if self.reconcile_catalog_manifest_inventory(manifest)? != manifest.entries {
            return Err(DbError::Pipeline(
                "installed catalog differs from its sealed schema contracts".into(),
            ));
        }
        guard.sealed();
        Ok(results)
    }
}

fn parse_bootstrap(db: &LaminarDB, sql: &[String]) -> Result<Vec<BootstrapEntry>, DbError> {
    if sql.len() > 4096
        || sql
            .iter()
            .map(String::len)
            .try_fold(0_usize, usize::checked_add)
            .is_none_or(|size| size > 8 * 1024 * 1024)
    {
        return Err(DbError::InvalidOperation(
            "catalog bootstrap exceeds 4096 entries or 8 MiB of original SQL".into(),
        ));
    }
    let mut entries = Vec::new();
    let mut names = HashSet::new();
    for batch in sql {
        for sql in crate::sql_utils::split_statements(batch) {
            let mut statements = parse_streaming_sql(sql)?;
            if statements.len() != 1 {
                return Err(DbError::InvalidOperation(
                    "cluster bootstrap entries must contain exactly one SQL statement".into(),
                ));
            }
            let statement = statements
                .pop()
                .ok_or_else(|| DbError::InvalidOperation("empty bootstrap statement".into()))?;
            let (name, kind, _) = validate_cluster_catalog_create(db, sql, &statement)?;
            if !names.insert(name.clone()) {
                return Err(DbError::InvalidOperation(format!(
                    "cluster bootstrap defines '{name}' more than once"
                )));
            }
            if entries.len() >= 4096 {
                return Err(DbError::InvalidOperation(
                    "catalog bootstrap exceeds 4096 parsed entries".into(),
                ));
            }
            entries.push((sql.to_owned(), statement, name, kind));
        }
    }
    Ok(entries)
}

impl LaminarDB {
    pub(crate) fn preflight_cluster_catalog_mutation(
        &self,
        sql: &str,
        statement: &StreamingStatement,
    ) -> Result<(), DbError> {
        if !super::is_topology_ddl(statement) || super::catalog_manifest_replay_active() {
            return Ok(());
        }
        let store_configured = self.catalog_manifest_store.lock().is_some();
        if !store_configured && !self.is_cluster_runtime() {
            return Ok(());
        }
        validate_cluster_catalog_create(self, sql, statement)?;
        if !store_configured && !super::catalog_bootstrap_active() {
            return Err(DbError::Pipeline(
                "cluster topology DDL requires a catalog manifest store".into(),
            ));
        }
        if !super::catalog_bootstrap_active() {
            return Err(DbError::Pipeline(
                "[LDB-6043] configured cluster topology can change only through startup bootstrap/replay until a replicated topology-version barrier is implemented".into(),
            ));
        }
        Ok(())
    }
}
