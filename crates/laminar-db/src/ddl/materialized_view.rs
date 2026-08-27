//! Materialized-view statements: schema resolution, incremental-emit admission,
//! provider registration, and create/drop orchestration.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::StreamingStatement;

use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};
use crate::pipeline::{ControlMutation, ControlMutationState};

use super::catalog::{reject_reserved_namespace, CatalogNameReservation};
use super::cluster_checks::{ChangelogInputKind, IntervalTopologyCandidate};
use super::control::resolve_control_ack;
use super::stream::{
    query_inputs_registered, validate_managed_aggregate_admission, PlannedStreamingQuery,
};

/// Incremental-emit store decision for a non-windowed MV.
enum IncEmit {
    /// Keyed running aggregate → keyed upsert snapshot (key = GROUP BY column indices).
    Upsert(Vec<usize>),
    /// Projection/filter over a changelog → Z-set multiset snapshot.
    Multiset,
    /// Full-emit (not incremental): replace-all aggregate or append.
    None,
}

struct MaterializedViewCreateGuard<'a> {
    db: &'a LaminarDB,
    name: String,
    mutation: Arc<ControlMutation>,
}

impl Drop for MaterializedViewCreateGuard<'_> {
    fn drop(&mut self) {
        if self.mutation.cancel() != ControlMutationState::Applied {
            self.db.rollback_catalog_create_or_fence(
                &self.name,
                CatalogObjectKind::MaterializedView,
                "materialized-view create rollback",
            );
        }
    }
}

/// Terminality guard error: `consumer` tried to read incremental MV `mv`'s changelog.
pub(crate) fn incremental_mv_consumer_error(mv: &str, consumer: &str) -> DbError {
    DbError::MaterializedView(format!(
        "[LDB-1300] {consumer} cannot consume incremental materialized view '{mv}': it emits a \
         dirty-only changelog, not a full snapshot. Read it with `SELECT * FROM {mv}` (snapshot), \
         or recreate '{mv}' without `incremental_emit`."
    ))
}

impl LaminarDB {
    /// Register a materialized view and wire it into the running pipeline.
    pub(crate) async fn handle_create_materialized_view(
        &self,
        sql: &str,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<laminar_sql::parser::EmitClause>,
        or_replace: bool,
        if_not_exists: bool,
        query_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("CREATE MATERIALIZED VIEW")?;
        let name_str = canonical_object_name(name)?;
        reject_reserved_namespace(&name_str)?;
        if or_replace {
            return Err(DbError::InvalidOperation(
                "CREATE OR REPLACE MATERIALIZED VIEW is not atomic; use DROP MATERIALIZED VIEW followed by CREATE MATERIALIZED VIEW"
                    .into(),
            ));
        }
        if self.is_cluster_runtime() {
            return Err(Self::cluster_state_lifecycle_error(
                "materialized view",
                &name_str,
                "materialized state has no planner-certified distribution and assignment-fenced checkpoint/read lifecycle",
            ));
        }
        Self::reject_interval_output_subquery(&name_str, query_sql)?;
        let Some(reservation) = self.reserve_catalog_name(
            &name_str,
            CatalogObjectKind::MaterializedView,
            if_not_exists,
        )?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE MATERIALIZED VIEW".to_string(),
                object_name: name_str,
                applied: false,
            }));
        };

        let (planned, interval_topology) = self
            .validate_mv_admission(name, query, emit_clause, query_sql)
            .await?;
        self.register_and_admit_mv(
            sql,
            &name_str,
            reservation,
            planned,
            interval_topology,
            query_sql,
        )
        .await?;

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE MATERIALIZED VIEW".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    /// Every pre-mutation MV admission check, returning the planned query and
    /// its interval-topology classification.
    async fn validate_mv_admission(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<laminar_sql::parser::EmitClause>,
        query_sql: &str,
    ) -> Result<(PlannedStreamingQuery, IntervalTopologyCandidate), DbError> {
        let name_str = canonical_object_name(name)?;
        let planned = self
            .plan_streaming_query(name, query, emit_clause, query_sql)
            .await?;
        if !self.is_cluster_runtime()
            && (planned.window_config.is_some() || planned.join_config.is_none())
            && query_inputs_registered(&self.ctx, query_sql)
        {
            let _ = validate_managed_aggregate_admission(
                &self.ctx,
                query_sql,
                planned.window_config.as_ref(),
                planned.emit_clause.as_ref(),
                self.checkpoint_key_groups(),
            )
            .await?;
        }
        Self::reject_temporal_materialized_view(query_sql, &planned)?;
        let _ = self
            .validate_temporal_topology_candidate(&name_str, query_sql, &planned)
            .await?;
        self.validate_interval_join_schema(&name_str, query_sql, &planned)
            .await?;
        let interval_topology = self
            .validate_interval_topology_candidate(
                "materialized view",
                &name_str,
                query_sql,
                &planned,
            )
            .await?;
        Ok((planned, interval_topology))
    }

    /// MV registry + stream registration, provider wiring, and the coordinator
    /// control admission round. The create guard lives here so its rollback
    /// fence covers exactly this phase; `reservation` commits only after the
    /// admission acknowledgement resolves.
    async fn register_and_admit_mv(
        &self,
        sql: &str,
        name_str: &str,
        mut reservation: CatalogNameReservation<'_>,
        planned: PlannedStreamingQuery,
        interval_topology: IntervalTopologyCandidate,
        query_sql: &str,
    ) -> Result<(), DbError> {
        let PlannedStreamingQuery {
            emit_clause: plan_emit,
            window_config: plan_window,
            order_config: plan_order,
            join_config: plan_joins,
            has_analytic: plan_has_analytic,
            has_frame: plan_has_frame,
            subscription_output: _,
        } = planned;
        let query_sql = query_sql.to_string();
        // A chained MV over an incremental MV must net the changelog — a non-windowed aggregate or
        // a simple projection/filter; a complex shape (e.g. a join) is rejected.
        self.reject_unsupported_reading_incremental_mv(
            &query_sql,
            "a materialized view",
            plan_window.is_some(),
        )
        .await?;
        let schema = self.resolve_mv_schema(&query_sql).await?;
        let sources = self.collect_mv_sources(&query_sql, name_str);

        {
            let mv =
                laminar_core::mv::MaterializedView::new(name_str, sql, sources, schema.clone());

            let mut registry = self.mv_registry.lock();

            registry
                .register(mv)
                .map_err(|e| DbError::MaterializedView(e.to_string()))?;
        }
        let mutation = Arc::new(ControlMutation::new());
        reservation.bind_control_mutation(Arc::clone(&mutation));
        let _create_guard = MaterializedViewCreateGuard {
            db: self,
            name: name_str.to_string(),
            mutation: Arc::clone(&mutation),
        };

        // An incremental MV emits a dirty-only changelog into a snapshot store; decide the store once
        // so the operator and MV store agree (keyed upsert for aggregates, Z-set for proj/filter).
        let (inc, has_aggregate) = if interval_topology.mutable_join {
            (IncEmit::Multiset, false)
        } else {
            self.incremental_emit_mode(&query_sql, plan_window.is_some(), &interval_topology)
                .await
        };
        let incremental = !matches!(inc, IncEmit::None);

        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.to_string(),
                query_sql: query_sql.clone(),
                emit_clause: plan_emit.clone(),
                window_config: plan_window.clone(),
                order_config: plan_order.clone(),
                join_config: plan_joins.clone(),
                has_analytic: plan_has_analytic,
                has_frame: plan_has_frame,
                incremental,
                // Cluster materialized-view subscriptions remain fail-closed.
                subscription_output: None,
                subscription_retention_bytes: 0,
                catalog_generation: 1,
                subscription_certificate: None,
            });
            mgr.store_ddl(name_str, sql);
        }

        self.register_mv_provider(name_str, &schema, plan_window.is_some(), inc, has_aggregate)?;

        let admission = {
            let guard = self.control_tx.lock();
            guard.as_ref().map(|tx| {
                let (reply, admission) = tokio::sync::oneshot::channel();
                tx.try_send(crate::pipeline::ControlMsg::add_stream(
                    name_str.to_string(),
                    query_sql,
                    plan_emit,
                    plan_window,
                    plan_order,
                    plan_joins,
                    incremental,
                    reply,
                    Arc::clone(&mutation),
                ))
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "control channel busy, retry CREATE MATERIALIZED VIEW '{name_str}': {e}"
                    ))
                })?;
                Ok::<_, DbError>(admission)
            })
        }
        .transpose();
        let admission_result = match admission {
            Ok(Some(admission)) => {
                resolve_control_ack(
                    &format!("CREATE MATERIALIZED VIEW '{name_str}'"),
                    admission,
                    &mutation,
                )
                .await
            }
            Ok(None) => self.apply_without_live_control(
                &format!("CREATE MATERIALIZED VIEW '{name_str}'"),
                &mutation,
            ),
            Err(error) => Err(error),
        };
        admission_result?;

        reservation.commit();
        Ok(())
    }

    /// Falls back to executing the query when static schema planning is unavailable.
    async fn resolve_mv_schema(&self, query_sql: &str) -> Result<Arc<Schema>, DbError> {
        if let Some(s) = crate::pipeline_lifecycle::plan_output_schema(&self.ctx, query_sql).await {
            return Ok(s);
        }
        Ok(match self.handle_query(query_sql).await? {
            ExecuteResult::Query(qh) => qh.schema().clone(),
            _ => Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Utf8,
                true,
            )])),
        })
    }

    fn collect_mv_sources(&self, query_sql: &str, name_str: &str) -> Vec<String> {
        let table_refs = crate::sql_analysis::extract_table_references(query_sql);
        let mut sources: Vec<String> = self
            .catalog
            .list_sources()
            .into_iter()
            .filter(|s| table_refs.contains(s.as_str()))
            .collect();
        let registry = self.mv_registry.lock();
        for view in registry.views() {
            if view.name != name_str && table_refs.contains(view.name.as_str()) {
                sources.push(view.name.clone());
            }
        }
        sources
    }

    /// The first table reference in `query_sql` that is an incremental MV, if any.
    pub(super) fn first_incremental_ref(&self, query_sql: &str) -> Option<String> {
        let refs = crate::sql_analysis::extract_table_references(query_sql);
        let mgr = self.connector_manager.lock();
        refs.iter()
            .find(|r| mgr.streams().get(r.as_str()).is_some_and(|s| s.incremental))
            .cloned()
    }

    /// All registered incremental MV (changelog producer) names.
    fn incremental_mv_names(&self) -> rustc_hash::FxHashSet<String> {
        self.connector_manager
            .lock()
            .streams()
            .iter()
            .filter(|(_, r)| r.incremental)
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Static (reference/dimension) table names — valid right sides for a changelog enrich join.
    pub(super) fn static_table_names(&self) -> rustc_hash::FxHashSet<String> {
        let on_demand: rustc_hash::FxHashSet<String> = self
            .connector_manager
            .lock()
            .tables()
            .values()
            .filter(|registration| registration.on_demand)
            .map(|registration| registration.name.clone())
            .collect();
        self.table_store
            .read()
            .table_names()
            .into_iter()
            .filter(|name| !on_demand.contains(name))
            .collect()
    }

    /// A query reading an incremental MV must net the retraction changelog — a non-windowed
    /// aggregate or a simple projection/filter; other stateful shapes are rejected.
    pub(super) async fn reject_unsupported_reading_incremental_mv(
        &self,
        query_sql: &str,
        consumer: &str,
        has_window: bool,
    ) -> Result<(), DbError> {
        let Some(mv) = self.first_incremental_ref(query_sql) else {
            return Ok(());
        };
        if has_window {
            return Err(incremental_mv_consumer_error(&mv, consumer));
        }
        // A changelog may enrich against a static table. Every other join shape is rejected;
        // non-windowed aggregates and simple projection/filter consumers continue to net
        // retractions.
        let inc = self.incremental_mv_names();
        let changelog_enrich = crate::sql_analysis::detect_changelog_enrich_query(
            query_sql,
            &inc,
            &self.static_table_names(),
        )
        .is_some();
        if crate::sql_analysis::has_join_clause(query_sql) && !changelog_enrich {
            return Err(incremental_mv_consumer_error(&mv, consumer));
        }
        let supported = changelog_enrich
            || self.ctx.sql(query_sql).await.is_ok_and(|df| {
                let plan = df.logical_plan();
                crate::aggregate_state::find_aggregate(plan).is_some()
                    || crate::sql_analysis::extract_projection_filter(plan).is_some()
            });
        if supported {
            Ok(())
        } else {
            Err(incremental_mv_consumer_error(&mv, consumer))
        }
    }

    /// Store decision for a non-windowed MV: keyed `Upsert` for a keyed aggregate, `Multiset` for a
    /// projection/filter over a changelog, `None` (full-emit) otherwise (incl. global aggregates).
    /// Returns the store mode and whether the query is an aggregate (threaded to
    /// `register_mv_provider` so it needn't re-plan to pick `Aggregate` vs append storage).
    async fn incremental_emit_mode(
        &self,
        query_sql: &str,
        has_window: bool,
        topology: &IntervalTopologyCandidate,
    ) -> (IncEmit, bool) {
        if has_window {
            return (IncEmit::None, false);
        }
        let flag = self.config.incremental_emit;
        let reads_incremental =
            topology.changelog_input.is_some() || self.first_incremental_ref(query_sql).is_some();
        let Some(df) = self.ctx.sql(query_sql).await.ok() else {
            return (IncEmit::None, false);
        };
        let plan = df.logical_plan();
        if let Some(agg) = crate::aggregate_state::find_aggregate(plan) {
            let n = agg.group_exprs.len();
            // Keyed aggregate → upsert (terminal under the flag, or chained over an incremental
            // MV). A global aggregate (no GROUP BY) is single-row → full-emit.
            let inc = if n > 0 && (flag || reads_incremental) {
                IncEmit::Upsert((0..n).collect())
            } else {
                IncEmit::None
            };
            return (inc, true);
        }
        // Projection/filter over an incremental MV's changelog → Z-set multiset snapshot.
        if topology.candidate_carries_changelog
            && crate::sql_analysis::extract_projection_filter(plan).is_some()
        {
            return (IncEmit::Multiset, false);
        }
        // `changelog ⋈ static dim` enrich join → Z-set multiset snapshot.
        if topology.candidate_carries_changelog
            && (matches!(
                topology.changelog_input,
                Some(ChangelogInputKind::StaticEnrich)
            ) || crate::sql_analysis::detect_changelog_enrich_query(
                query_sql,
                &self.incremental_mv_names(),
                &self.static_table_names(),
            )
            .is_some())
        {
            return (IncEmit::Multiset, false);
        }
        (IncEmit::None, false)
    }

    fn register_mv_provider(
        &self,
        name_str: &str,
        schema: &Arc<Schema>,
        has_window: bool,
        inc: IncEmit,
        has_aggregate: bool,
    ) -> Result<(), DbError> {
        use crate::mv_store::MvStorageMode;

        // Incremental MVs maintain a snapshot from a dirty-only changelog. Otherwise: non-windowed
        // aggs replace-all every cycle; windowed aggs append (preserving prior windows), as do non-aggregates.
        let mode = match inc {
            IncEmit::Upsert(key_cols) => MvStorageMode::Upsert { key_cols },
            IncEmit::Multiset => MvStorageMode::Multiset,
            IncEmit::None if has_aggregate && !has_window => MvStorageMode::Aggregate,
            IncEmit::None => MvStorageMode::append_default(),
        };

        self.mv_store
            .write()
            .create_mv(name_str, schema.clone(), mode)?;

        let provider: Arc<dyn datafusion::datasource::TableProvider> =
            Arc::new(crate::table_provider::MvTableProvider::new(
                name_str.to_string(),
                schema.clone(),
                self.mv_store.clone(),
            ));

        match self
            .ctx
            .register_table(exact_table_reference(name_str), provider)
        {
            Ok(None) => Ok(()),
            Ok(Some(previous)) => {
                let _ = self
                    .ctx
                    .register_table(exact_table_reference(name_str), previous);
                Err(DbError::MaterializedView(format!(
                    "cannot create materialized view '{name_str}': the table namespace was \
                     claimed concurrently"
                )))
            }
            Err(error) => Err(DbError::MaterializedView(format!(
                "Failed to register MV table provider: {error}"
            ))),
        }
    }
}
