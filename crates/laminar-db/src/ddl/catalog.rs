//! Catalog namespace ownership, create/drop cleanup, rollback fencing, and
//! topology-DDL admission gates.
//!
//! INVARIANT: a failed or partially verified cleanup fences this `LaminarDB`
//! instance terminally ([LDB-6044]) — cleanup is never retried or guessed.

use std::sync::Arc;

use laminar_core::catalog::CatalogObjectKind;

use crate::db::{exact_table_reference, DbState, LaminarDB};
use crate::error::DbError;
use crate::pipeline::{ControlMutation, ControlMutationState};

pub(crate) struct CatalogNameReservation<'a> {
    db: &'a LaminarDB,
    name: String,
    kind: CatalogObjectKind,
    control_mutation: Option<Arc<ControlMutation>>,
    committed: bool,
}

impl CatalogNameReservation<'_> {
    pub(super) fn bind_control_mutation(&mut self, mutation: Arc<ControlMutation>) {
        debug_assert!(self.control_mutation.is_none());
        self.control_mutation = Some(mutation);
    }

    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for CatalogNameReservation<'_> {
    fn drop(&mut self) {
        let applied = self
            .control_mutation
            .as_ref()
            .is_some_and(|mutation| mutation.state() == ControlMutationState::Applied);
        if !self.committed && !applied {
            self.db.rollback_catalog_create_or_fence(
                &self.name,
                self.kind,
                "catalog create rollback",
            );
        }
    }
}

/// Connector-manager registration presence for one name, used by residue
/// verification. The flags mirror one connector-manager snapshot and are only
/// meaningful together.
#[derive(Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct ConnectorPresence {
    source: bool,
    sink: bool,
    stream: bool,
    table: bool,
}

/// Reject object names in the reserved `laminar` namespace, which is owned by
/// the system catalog (`laminar.models`, `laminar.ai_calls`).
pub(super) fn reject_reserved_namespace(name: &str) -> Result<(), DbError> {
    if name.starts_with("laminar.") {
        return Err(DbError::InvalidOperation(format!(
            "'{name}' uses the reserved 'laminar' namespace (system catalog views \
             laminar.models / laminar.ai_calls live there)"
        )));
    }
    Ok(())
}

impl LaminarDB {
    fn catalog_object_is_present(
        &self,
        name: &str,
        kind: CatalogObjectKind,
    ) -> Result<bool, DbError> {
        match kind {
            CatalogObjectKind::Source => Ok(self.catalog.get_source(name).is_some()
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify source provider for '{name}': {error}"
                        ))
                    })?
                && self.planner.lock().get_source(name).is_some()
                && self.mv_registry.lock().is_base_table(name)),
            CatalogObjectKind::Sink => Ok(self.catalog.get_sink_input(name).is_some()
                && self.planner.lock().get_sink(name).is_some()),
            CatalogObjectKind::Table => Ok(self.table_store.read().has_table(name)
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify table provider for '{name}': {error}"
                        ))
                    })?),
            CatalogObjectKind::LookupTable => Ok(self.table_store.read().has_table(name)
                && self.planner.lock().get_lookup_table(name).is_some()
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify lookup provider for '{name}': {error}"
                        ))
                    })?),
            CatalogObjectKind::Stream => Ok(self.catalog.get_stream_entry(name).is_some()
                && self.connector_manager.lock().streams().contains_key(name)),
            CatalogObjectKind::MaterializedView => Ok(self.mv_registry.lock().get(name).is_some()
                && self.connector_manager.lock().streams().contains_key(name)
                && self.mv_store.read().has_mv(name)
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify materialized-view provider for '{name}': {error}"
                        ))
                    })?),
        }
    }

    pub(crate) fn reserve_catalog_name(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        if_not_exists: bool,
    ) -> Result<Option<CatalogNameReservation<'_>>, DbError> {
        self.ensure_catalog_cleanup_unfenced("catalog create")?;
        reject_reserved_namespace(name)?;
        let existing = self.catalog_namespace.lock().get(name).copied();
        if let Some(existing) = existing {
            if existing != kind {
                return Err(DbError::InvalidOperation(format!(
                    "cannot create {kind} '{name}': the identifier is owned by a {existing}"
                )));
            }
            if !self.catalog_object_is_present(name, kind)? {
                return Err(DbError::InvalidOperation(format!(
                    "catalog namespace for {kind} '{name}' is inconsistent"
                )));
            }
            if if_not_exists {
                return Ok(None);
            }
            return Err(DbError::InvalidOperation(format!(
                "{kind} '{name}' already exists"
            )));
        }

        if self
            .ctx
            .table_exist(exact_table_reference(name))
            .map_err(|error| {
                DbError::InvalidOperation(format!(
                    "could not inspect catalog namespace for '{name}': {error}"
                ))
            })?
        {
            return Err(DbError::InvalidOperation(format!(
                "cannot create {kind} '{name}': an untyped table provider already owns the identifier"
            )));
        }

        let replaced = self.catalog_namespace.lock().insert(name.to_string(), kind);
        debug_assert!(replaced.is_none());
        Ok(Some(CatalogNameReservation {
            db: self,
            name: name.to_string(),
            kind,
            control_mutation: None,
            committed: false,
        }))
    }

    pub(crate) fn require_catalog_kind(
        &self,
        name: &str,
        expected: CatalogObjectKind,
        if_exists: bool,
    ) -> Result<bool, DbError> {
        self.ensure_catalog_cleanup_unfenced("catalog drop")?;
        let Some(actual) = self.catalog_namespace.lock().get(name).copied() else {
            if if_exists {
                return Ok(false);
            }
            return Err(match expected {
                CatalogObjectKind::Source => DbError::SourceNotFound(name.to_string()),
                CatalogObjectKind::Sink => DbError::SinkNotFound(name.to_string()),
                CatalogObjectKind::Table => DbError::TableNotFound(name.to_string()),
                CatalogObjectKind::Stream => DbError::StreamNotFound(name.to_string()),
                CatalogObjectKind::MaterializedView => {
                    DbError::MaterializedView(format!("materialized view not found: {name}"))
                }
                CatalogObjectKind::LookupTable => {
                    DbError::InvalidOperation(format!("lookup table '{name}' does not exist"))
                }
            });
        };
        if actual != expected {
            return Err(DbError::InvalidOperation(format!(
                "cannot drop {expected} '{name}': the identifier is owned by a {actual}"
            )));
        }
        if !self.catalog_object_is_present(name, expected)? {
            return Err(DbError::InvalidOperation(format!(
                "catalog namespace for {expected} '{name}' is inconsistent"
            )));
        }
        Ok(true)
    }

    fn deregister_catalog_provider(&self, name: &str, errors: &mut Vec<String>) {
        #[cfg(test)]
        if self.catalog_cleanup_deregister_fault.lock().as_deref() == Some(name) {
            errors.push("injected DataFusion provider deregistration failure".into());
            return;
        }

        if let Err(error) = self.ctx.deregister_table(exact_table_reference(name)) {
            errors.push(format!(
                "DataFusion provider deregistration failed: {error}"
            ));
        }
    }

    /// Per-kind registry residues after the shared provider/DDL checks.
    #[allow(clippy::type_complexity)]
    fn catalog_kind_residues(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        presence: ConnectorPresence,
    ) -> Vec<&'static str> {
        let mut residues = Vec::new();
        match kind {
            CatalogObjectKind::Source => {
                if self.catalog.get_source(name).is_some() {
                    residues.push("source catalog");
                }
                if presence.source {
                    residues.push("source connector registration");
                }
                if self.planner.lock().get_source(name).is_some() {
                    residues.push("source planner registration");
                }
                if self.mv_registry.lock().is_base_table(name) {
                    residues.push("materialized-view base-table registration");
                }
            }
            CatalogObjectKind::Sink => {
                if self.catalog.get_sink_input(name).is_some() {
                    residues.push("sink catalog");
                }
                if presence.sink {
                    residues.push("sink connector registration");
                }
                if self.planner.lock().get_sink(name).is_some() {
                    residues.push("sink planner registration");
                }
            }
            CatalogObjectKind::Table => {
                if self.table_store.read().has_table(name) {
                    residues.push("table store");
                }
                if presence.table {
                    residues.push("table connector registration");
                }
            }
            CatalogObjectKind::LookupTable => {
                if self.table_store.read().has_table(name) {
                    residues.push("table store");
                }
                if presence.table {
                    residues.push("table connector registration");
                }
                if self.lookup_registry.get_entry(name).is_some() {
                    residues.push("lookup snapshot registry");
                }
                if self.planner.lock().get_lookup_table(name).is_some() {
                    residues.push("lookup planner registration");
                }
            }
            CatalogObjectKind::Stream => {
                if self.catalog.get_stream_entry(name).is_some() {
                    residues.push("stream catalog");
                }
                if presence.stream {
                    residues.push("stream registration");
                }
                if self.subscription_registry.contains_name(name) {
                    residues.push("subscription registry");
                }
                if self.planner.lock().has_query(name) {
                    residues.push("query planner registration");
                }
                if self.stream_schemas.read().contains_key(name) {
                    residues.push("stream schema cache");
                }
            }
            CatalogObjectKind::MaterializedView => {
                if self.mv_registry.lock().get(name).is_some() {
                    residues.push("materialized-view registry");
                }
                if presence.stream {
                    residues.push("stream registration");
                }
                if self.mv_store.read().has_mv(name) {
                    residues.push("materialized-view store");
                }
                if self.subscription_registry.contains_name(name) {
                    residues.push("subscription registry");
                }
                if self.planner.lock().has_query(name) {
                    residues.push("query planner registration");
                }
                if self.stream_schemas.read().contains_key(name) {
                    residues.push("stream schema cache");
                }
            }
        }
        residues
    }

    /// Residual registrations that prove a cleanup was incomplete: `DataFusion`
    /// providers, stored DDL, and every per-kind registry entry. Errors from
    /// the verification reads themselves are appended to `errors`.
    fn catalog_cleanup_residues(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        errors: &mut Vec<String>,
    ) -> Vec<&'static str> {
        let mut residues = Vec::new();
        if matches!(
            kind,
            CatalogObjectKind::Source
                | CatalogObjectKind::Table
                | CatalogObjectKind::LookupTable
                | CatalogObjectKind::Stream
                | CatalogObjectKind::MaterializedView
        ) {
            match self.ctx.table_exist(exact_table_reference(name)) {
                Ok(false) => {}
                Ok(true) => residues.push("DataFusion provider"),
                Err(error) => errors.push(format!(
                    "DataFusion provider absence verification failed: {error}"
                )),
            }
        }

        let (has_ddl, presence) = {
            let manager = self.connector_manager.lock();
            (
                manager.get_ddl(name).is_some(),
                ConnectorPresence {
                    source: manager.sources().contains_key(name),
                    sink: manager.sinks().contains_key(name),
                    stream: manager.streams().contains_key(name),
                    table: manager.tables().contains_key(name),
                },
            )
        };
        if has_ddl {
            residues.push("stored DDL");
        }
        residues.extend(self.catalog_kind_residues(name, kind, presence));
        residues
    }

    fn cleanup_catalog_object(&self, name: &str, kind: CatalogObjectKind) -> Result<(), DbError> {
        let mut errors = Vec::new();

        if matches!(
            kind,
            CatalogObjectKind::Source
                | CatalogObjectKind::Table
                | CatalogObjectKind::LookupTable
                | CatalogObjectKind::Stream
                | CatalogObjectKind::MaterializedView
        ) {
            self.deregister_catalog_provider(name, &mut errors);
        }

        match kind {
            CatalogObjectKind::Source => {
                self.catalog.drop_source(name);
                self.connector_manager.lock().unregister_source(name);
                self.planner.lock().unregister_source(name);
                self.mv_registry.lock().unregister_base_table(name);
            }
            CatalogObjectKind::Sink => {
                self.catalog.drop_sink(name);
                self.connector_manager.lock().unregister_sink(name);
                self.planner.lock().unregister_sink(name);
            }
            CatalogObjectKind::Table => {
                self.table_store.write().drop_table(name);
                self.connector_manager.lock().unregister_table(name);
            }
            CatalogObjectKind::LookupTable => {
                self.table_store.write().drop_table(name);
                self.connector_manager.lock().unregister_table(name);
                self.lookup_registry.unregister(name);
                self.planner.lock().unregister_lookup_table(name);
                self.refresh_lookup_optimizer_rule();
            }
            CatalogObjectKind::Stream => {
                self.catalog.drop_stream(name);
                self.connector_manager.lock().unregister_stream(name);
                self.subscription_registry.drop_name(name);
                self.planner.lock().unregister_query(name);
                self.stream_schemas.write().remove(name);
            }
            CatalogObjectKind::MaterializedView => {
                let mut registry = self.mv_registry.lock();
                if registry.get(name).is_some() {
                    if let Err(error) = registry.unregister(name) {
                        errors.push(format!(
                            "materialized-view registry deregistration failed: {error}"
                        ));
                    }
                }
                drop(registry);
                self.connector_manager.lock().unregister_stream(name);
                self.mv_store.write().drop_mv(name);
                self.subscription_registry.drop_name(name);
                self.planner.lock().unregister_query(name);
                self.stream_schemas.write().remove(name);
            }
        }
        self.connector_manager.lock().remove_ddl(name);

        let residues = self.catalog_cleanup_residues(name, kind, &mut errors);
        if !errors.is_empty() || !residues.is_empty() {
            let mut details = errors;
            if !residues.is_empty() {
                details.push(format!("residual state: {}", residues.join(", ")));
            }
            return Err(DbError::InvalidOperation(format!(
                "could not prove complete cleanup of {kind} '{name}': {}",
                details.join("; ")
            )));
        }

        let mut namespace = self.catalog_namespace.lock();
        match namespace.get(name).copied() {
            Some(owner) if owner != kind => {
                return Err(DbError::InvalidOperation(format!(
                    "cannot release cleanup ownership for {kind} '{name}': namespace is owned by {owner}"
                )));
            }
            Some(_) => {
                namespace.remove(name);
            }
            None => {}
        }
        Ok(())
    }

    fn terminal_catalog_cleanup_error(
        &self,
        context: &str,
        name: &str,
        kind: CatalogObjectKind,
        error: &DbError,
    ) -> DbError {
        let reason = format!(
            "[LDB-6044] {context} left {kind} '{name}' incompletely cleaned; this LaminarDB instance is permanently fenced: {error}"
        );
        let was_fenced = self
            .catalog_cleanup_fenced
            .swap(true, std::sync::atomic::Ordering::AcqRel);
        let recorded = {
            let mut fault = self.last_fault.lock();
            if !was_fenced || fault.is_none() {
                *fault = Some(reason);
            }
            fault.clone().unwrap_or_else(|| {
                "[LDB-6044] catalog cleanup failed and the instance is permanently fenced".into()
            })
        };
        #[cfg(feature = "cluster")]
        if self.is_cluster_runtime() {
            self.latch_local_terminal_pipeline_halt();
            if let Some(controller) = self.cluster_controller.lock().clone() {
                controller.set_recovering(true);
                if let Err(publication_error) = crate::coordinated_recovery::queue_local_fault(
                    &controller,
                    &self.pending_recovery_fault,
                ) {
                    tracing::error!(
                        %publication_error,
                        "could not retain terminal catalog-cleanup fault request"
                    );
                }
            }
        } else {
            self.terminal_pipeline_halt
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        #[cfg(not(feature = "cluster"))]
        self.terminal_pipeline_halt
            .store(true, std::sync::atomic::Ordering::SeqCst);
        DbState::Faulted.store(&self.state);
        self.shutdown_signal.notify_one();
        tracing::error!(reason = %recorded, "catalog cleanup terminally fenced the database");
        DbError::PipelineTerminal(recorded)
    }

    pub(crate) fn rollback_catalog_create(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        context: &str,
    ) -> Result<(), DbError> {
        self.cleanup_catalog_object(name, kind)
            .map_err(|error| self.terminal_catalog_cleanup_error(context, name, kind, &error))
    }

    pub(crate) fn rollback_catalog_create_or_fence(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        context: &str,
    ) {
        if let Err(error) = self.rollback_catalog_create(name, kind, context) {
            tracing::error!(%error, "catalog rollback guard could not complete cleanup");
        }
    }

    pub(crate) fn ensure_catalog_cleanup_unfenced(&self, operation: &str) -> Result<(), DbError> {
        if !self
            .catalog_cleanup_fenced
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        let reason = self.last_fault.lock().clone().unwrap_or_else(|| {
            "[LDB-6044] catalog cleanup is incomplete and this LaminarDB instance is permanently fenced"
                .into()
        });
        Err(DbError::PipelineTerminal(format!(
            "{operation} rejected by terminal catalog cleanup fence: {reason}"
        )))
    }

    /// Streams and materialized views have a synchronous coordinator control path.
    /// Lifecycle transitions never do; a running local, uncheckpointed pipeline is
    /// the only live topology that can admit them safely. Shape-specific handlers may
    /// still require offline initialization.
    pub(crate) fn ensure_topology_ddl_allowed(&self, operation: &str) -> Result<(), DbError> {
        self.ensure_catalog_cleanup_unfenced(operation)?;
        if crate::db::catalog_manifest_replay_active() {
            return Ok(());
        }
        match DbState::load(&self.state) {
            DbState::Starting | DbState::ShuttingDown | DbState::Faulted => {
                Err(DbError::Pipeline(format!(
                    "[LDB-6043] {operation} cannot change topology while the pipeline is \
                     starting, shutting down, or faulted; stop the pipeline first"
                )))
            }
            DbState::Running if self.is_cluster_runtime() => Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} cannot change an active cluster topology until a \
                 replicated topology-version barrier is implemented; stop the pipeline first"
            ))),
            DbState::Running if self.config.checkpoint.is_some() => {
                Err(DbError::Pipeline(format!(
                    "[LDB-6043] {operation} cannot change a running checkpointed topology; stop the \
                 pipeline and reset/migrate its checkpoint state first"
                )))
            }
            DbState::Running
                if self.connector_manager.lock().streams().values().any(|stream| {
                    stream.join_config.as_ref().is_some_and(|joins| {
                        joins.iter().any(|join| {
                            matches!(
                                join,
                                laminar_sql::translator::JoinOperatorConfig::Temporal(_)
                            )
                        })
                    })
                }) =>
            {
                Err(DbError::Pipeline(format!(
                    "[LDB-6043] {operation} cannot change a live topology containing managed temporal state; stop the pipeline before changing the topology"
                )))
            }
            DbState::Running if self.control_tx.lock().is_none() => {
                Err(DbError::Pipeline(format!(
                    "[LDB-6043] {operation} cannot change a running topology because its control \
                 coordinator is unavailable"
                )))
            }
            DbState::Created | DbState::Running | DbState::Stopped => Ok(()),
        }
    }

    pub(super) fn apply_without_live_control(
        &self,
        operation: &str,
        mutation: &ControlMutation,
    ) -> Result<(), DbError> {
        if crate::db::catalog_manifest_replay_active()
            || matches!(
                DbState::load(&self.state),
                DbState::Created | DbState::Stopped
            )
        {
            let applied = mutation.try_apply();
            debug_assert!(applied);
            Ok(())
        } else {
            Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} lost its live control coordinator before admission"
            )))
        }
    }

    /// Connector, source, sink, lookup, continuous-query, and table DDL has no
    /// coordinator control implementation. Reject it in every active runtime mode.
    pub(crate) fn ensure_offline_topology_ddl_allowed(
        &self,
        operation: &str,
    ) -> Result<(), DbError> {
        self.ensure_catalog_cleanup_unfenced(operation)?;
        if crate::db::catalog_manifest_replay_active() {
            return Ok(());
        }
        if matches!(
            DbState::load(&self.state),
            DbState::Starting | DbState::Running | DbState::ShuttingDown | DbState::Faulted
        ) {
            return Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} is not wired into a live runtime; stop the pipeline first"
            )));
        }
        Ok(())
    }
}
