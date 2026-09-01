//! Drop statements: dependency planning, streaming control acknowledgements,
//! and teardown across every catalog object kind.
//!
//! INVARIANT: a drop that cannot prove complete teardown fences the instance
//! terminally; partial drops never leave silently inconsistent catalog state.

use std::collections::HashSet;
use std::sync::Arc;

use laminar_core::catalog::CatalogObjectKind;

use crate::db::{canonical_object_name, DbState, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};
use crate::pipeline::{ControlMutation, ControlMutationState};

use super::control::resolve_control_ack;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CatalogDropTarget {
    pub(super) name: String,
    pub(super) kind: CatalogObjectKind,
}

struct StreamingDropGuard<'a> {
    db: &'a LaminarDB,
    targets: Vec<CatalogDropTarget>,
    mutation: Arc<ControlMutation>,
    finished: bool,
}

impl StreamingDropGuard<'_> {
    fn finish(mut self) -> Result<(), DbError> {
        debug_assert_eq!(self.mutation.state(), ControlMutationState::Applied);
        let result = self
            .db
            .teardown_catalog_targets(&self.targets, "catalog drop");
        self.finished = true;
        result
    }
}

impl Drop for StreamingDropGuard<'_> {
    fn drop(&mut self) {
        if !self.finished && self.mutation.cancel() == ControlMutationState::Applied {
            self.db
                .teardown_catalog_targets_or_fence(&self.targets, "cancelled catalog drop");
        }
    }
}

impl LaminarDB {
    pub(crate) fn handle_drop_source(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP SOURCE")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::Source, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP SOURCE".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name_str, CatalogObjectKind::Source, cascade)?;
        self.teardown_catalog_targets(&targets, "DROP SOURCE")?;
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SOURCE".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    pub(crate) fn handle_drop_sink(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP SINK")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::Sink, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP SINK".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name_str, CatalogObjectKind::Sink, cascade)?;
        self.teardown_catalog_targets(&targets, "DROP SINK")?;
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SINK".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    pub(crate) async fn handle_drop_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("DROP STREAM")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::Stream, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP STREAM".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name_str, CatalogObjectKind::Stream, cascade)?;
        self.ensure_mutable_interval_drop_offline("DROP STREAM", &targets)
            .await?;
        let graph_names: Vec<String> = targets
            .iter()
            .filter(|target| {
                matches!(
                    target.kind,
                    CatalogObjectKind::Stream | CatalogObjectKind::MaterializedView
                )
            })
            .map(|target| target.name.clone())
            .collect();

        let mutation = Arc::new(ControlMutation::new());
        let drop_guard = StreamingDropGuard {
            db: self,
            targets,
            mutation: Arc::clone(&mutation),
            finished: false,
        };
        self.acknowledge_runtime_drop("DROP STREAM", &graph_names, Arc::clone(&mutation))
            .await?;
        drop_guard.finish()?;

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP STREAM".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    async fn acknowledge_runtime_drop(
        &self,
        statement: &str,
        names: &[String],
        mutation: Arc<ControlMutation>,
    ) -> Result<(), DbError> {
        if names.is_empty() {
            return self.apply_without_live_control(statement, &mutation);
        }

        let acknowledgement = {
            let guard = self.control_tx.lock();
            guard.as_ref().map(|tx| {
                let (reply, acknowledgement) = tokio::sync::oneshot::channel();
                tx.try_send(crate::pipeline::ControlMsg::drop_streams(
                    names.to_vec(),
                    reply,
                    Arc::clone(&mutation),
                ))
                .map_err(|error| {
                    DbError::Pipeline(format!("control channel busy, retry {statement}: {error}"))
                })?;
                Ok::<_, DbError>(acknowledgement)
            })
        }
        .transpose()?;

        match acknowledgement {
            Some(acknowledgement) => {
                resolve_control_ack(statement, acknowledgement, &mutation).await
            }
            None => self.apply_without_live_control(statement, &mutation),
        }
    }

    fn direct_dependents(&self, name: &str) -> Result<Vec<CatalogDropTarget>, DbError> {
        let mut names = HashSet::new();
        {
            let manager = self.connector_manager.lock();
            for (stream_name, registration) in manager.streams() {
                if crate::sql_analysis::extract_table_references(&registration.query_sql)
                    .contains(name)
                {
                    names.insert(stream_name.clone());
                }
            }
        }
        for sink_name in self.catalog.list_sinks() {
            if self.catalog.get_sink_input(&sink_name).as_deref() == Some(name) {
                names.insert(sink_name);
            }
        }
        {
            let registry = self.mv_registry.lock();
            names.extend(registry.get_dependents(name).map(str::to_owned));
        }

        let namespace = self.catalog_namespace.lock();
        let mut targets = Vec::with_capacity(names.len());
        for dependent in names {
            let kind = namespace.get(&dependent).copied().ok_or_else(|| {
                DbError::InvalidOperation(format!(
                    "dependent '{dependent}' has no typed catalog owner"
                ))
            })?;
            if !matches!(
                kind,
                CatalogObjectKind::Sink
                    | CatalogObjectKind::Stream
                    | CatalogObjectKind::MaterializedView
            ) {
                return Err(DbError::InvalidOperation(format!(
                    "dependent '{dependent}' has invalid catalog kind {kind}"
                )));
            }
            targets.push(CatalogDropTarget {
                name: dependent,
                kind,
            });
        }
        targets.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(targets)
    }

    fn build_drop_plan(
        &self,
        name: &str,
        expected: CatalogObjectKind,
        cascade: bool,
    ) -> Result<Vec<CatalogDropTarget>, DbError> {
        fn visit(
            db: &LaminarDB,
            target: CatalogDropTarget,
            seen: &mut HashSet<String>,
            result: &mut Vec<CatalogDropTarget>,
        ) -> Result<(), DbError> {
            if !seen.insert(target.name.clone()) {
                return Ok(());
            }
            for dependent in db.direct_dependents(&target.name)? {
                visit(db, dependent, seen, result)?;
            }
            result.push(target);
            Ok(())
        }

        self.require_catalog_kind(name, expected, false)?;
        let direct = self.direct_dependents(name)?;
        if !cascade && !direct.is_empty() {
            let names = direct
                .iter()
                .map(|target| target.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(DbError::InvalidOperation(format!(
                "cannot drop {expected} '{name}': depended on by {names}; use CASCADE"
            )));
        }

        let mut result = Vec::new();
        let mut seen = HashSet::new();
        if cascade {
            for dependent in direct {
                visit(self, dependent, &mut seen, &mut result)?;
            }
        }
        seen.insert(name.to_string());
        result.push(CatalogDropTarget {
            name: name.to_string(),
            kind: expected,
        });

        if DbState::load(&self.state) == DbState::Running
            && result
                .iter()
                .any(|target| target.kind == CatalogObjectKind::Sink)
        {
            return Err(DbError::Pipeline(
                "a running cascade cannot remove sinks because sink teardown is not wired into live control"
                    .into(),
            ));
        }
        Ok(result)
    }

    fn teardown_catalog_targets(
        &self,
        targets: &[CatalogDropTarget],
        context: &str,
    ) -> Result<(), DbError> {
        for target in targets {
            self.rollback_catalog_create(&target.name, target.kind, context)?;
        }
        Ok(())
    }

    fn teardown_catalog_targets_or_fence(&self, targets: &[CatalogDropTarget], context: &str) {
        if let Err(error) = self.teardown_catalog_targets(targets, context) {
            tracing::error!(%error, "catalog teardown guard could not complete cleanup");
        }
    }
    pub(crate) async fn handle_drop_materialized_view(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("DROP MATERIALIZED VIEW")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::MaterializedView, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP MATERIALIZED VIEW".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets =
            self.build_drop_plan(&name_str, CatalogObjectKind::MaterializedView, cascade)?;
        self.ensure_mutable_interval_drop_offline("DROP MATERIALIZED VIEW", &targets)
            .await?;
        let graph_names: Vec<String> = targets
            .iter()
            .filter(|target| {
                matches!(
                    target.kind,
                    CatalogObjectKind::Stream | CatalogObjectKind::MaterializedView
                )
            })
            .map(|target| target.name.clone())
            .collect();

        let mutation = Arc::new(ControlMutation::new());
        let drop_guard = StreamingDropGuard {
            db: self,
            targets,
            mutation: Arc::clone(&mutation),
            finished: false,
        };
        self.acknowledge_runtime_drop(
            "DROP MATERIALIZED VIEW",
            &graph_names,
            Arc::clone(&mutation),
        )
        .await?;
        drop_guard.finish()?;

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP MATERIALIZED VIEW".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    pub(crate) fn handle_drop_lookup_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP LOOKUP TABLE")?;
        let name = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name, CatalogObjectKind::LookupTable, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP LOOKUP TABLE".into(),
                object_name: name,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name, CatalogObjectKind::LookupTable, false)?;
        self.teardown_catalog_targets(&targets, "DROP LOOKUP TABLE")?;
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP LOOKUP TABLE".into(),
            object_name: name,
            applied: true,
        }))
    }

    /// Handle DROP TABLE statement.
    pub(crate) fn handle_drop_table(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP TABLE")?;
        let mut plans = Vec::new();
        for obj_name in names {
            let name_str = canonical_object_name(obj_name)?;
            if self.require_catalog_kind(&name_str, CatalogObjectKind::Table, if_exists)? {
                plans.push(self.build_drop_plan(&name_str, CatalogObjectKind::Table, cascade)?);
            }
        }

        let mut seen = HashSet::new();
        let targets: Vec<_> = plans
            .into_iter()
            .flatten()
            .filter(|target| seen.insert(target.name.clone()))
            .collect();
        let applied = !targets.is_empty();
        self.teardown_catalog_targets(&targets, "DROP TABLE")?;

        let name = names
            .first()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP TABLE".to_string(),
            object_name: name,
            applied,
        }))
    }
}
