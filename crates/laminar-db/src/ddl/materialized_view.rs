//! Materialized-view statements: schema resolution, incremental-emit admission,
//! provider registration, and create/drop orchestration.

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};

use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::{MaterializedViewRetention, StreamingStatement};

use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};
use crate::pipeline::{ControlMutation, ControlMutationState};

use super::catalog::{reject_reserved_namespace, reject_stage_reads, CatalogNameReservation};
use super::cluster_checks::{ChangelogInputKind, IntervalTopologyCandidate};
use super::control::resolve_control_ack;
use super::stream::{
    query_inputs_registered, validate_managed_aggregate_admission, PlannedStreamingQuery,
};

/// What a view's stream emits into its store.
#[derive(Clone, Copy)]
enum ViewOutput {
    /// A weighted changelog kept as vnode slices; an aggregate upserts on its planned key.
    Changelog { aggregate: bool },
    /// Append-only rows with bounded retention.
    Append,
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
         changelog, not a full snapshot. Read it with `SELECT * FROM {mv}` (snapshot), or consume \
         its changelog with an aggregate or a projection."
    ))
}

impl LaminarDB {
    /// Register a materialized view and wire it into the running pipeline.
    pub(crate) async fn handle_create_materialized_view(
        &self,
        sql: &str,
        statement: &StreamingStatement,
    ) -> Result<ExecuteResult, DbError> {
        let StreamingStatement::CreateMaterializedView {
            name,
            query,
            emit_clause,
            or_replace,
            if_not_exists,
            query_sql,
            retention,
            freshness,
        } = statement
        else {
            return Err(DbError::InvalidOperation(
                "expected a CREATE MATERIALIZED VIEW statement".into(),
            ));
        };
        let (or_replace, if_not_exists) = (*or_replace, *if_not_exists);
        self.ensure_topology_ddl_allowed("CREATE MATERIALIZED VIEW")?;
        let name_str = canonical_object_name(name)?;
        reject_reserved_namespace(&name_str)?;
        reject_stage_reads(query_sql)?;
        if or_replace {
            return Err(DbError::InvalidOperation(
                "CREATE OR REPLACE MATERIALIZED VIEW is not atomic; use DROP MATERIALIZED VIEW followed by CREATE MATERIALIZED VIEW"
                    .into(),
            ));
        }
        Self::reject_interval_output_subquery(&name_str, query_sql)?;
        if let Some(freshness) = freshness {
            self.admit_view_freshness(&name_str, *freshness)?;
        }
        let Some(reservation) = self.reserve_catalog_name(
            &name_str,
            CatalogObjectKind::MaterializedView,
            if_not_exists,
        )?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE MATERIALIZED VIEW".to_string(),
                object_name: name_str,
                #[cfg(feature = "cluster")]
                topology_operation: None,
                applied: false,
            }));
        };

        let (planned, interval_topology) = self
            .validate_mv_admission(name, query, emit_clause.clone(), query_sql)
            .await?;
        self.register_and_admit_mv(
            sql,
            &name_str,
            reservation,
            (planned, interval_topology),
            query_sql,
            retention.as_ref(),
        )
        .await?;
        if let Some(freshness) = freshness {
            self.mv_store
                .read()
                .freshness_contracts()
                .declare(&name_str, *freshness);
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE MATERIALIZED VIEW".to_string(),
            object_name: name_str,
            #[cfg(feature = "cluster")]
            topology_operation: None,
            applied: true,
        }))
    }

    /// A view may promise committed reads no fresher than the checkpoint cadence that commits
    /// them.
    fn admit_view_freshness(&self, view: &str, freshness: Duration) -> Result<(), DbError> {
        let interval = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.interval_ms)
            .map(Duration::from_millis);
        let Some(interval) = interval else {
            return Err(DbError::InvalidOperation(format!(
                "view '{view}' declares freshness {freshness:?}, which needs a periodic \
                 checkpoint interval to commit its reads"
            )));
        };
        if freshness < interval {
            return Err(DbError::InvalidOperation(format!(
                "view '{view}' declares freshness {freshness:?}, below the checkpoint interval \
                 of {interval:?} that commits its reads"
            )));
        }
        Ok(())
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
        self.validate_cluster_query_shape_before_plan("materialized view", &name_str, query_sql)?;
        let planned = self
            .plan_streaming_query(name, query, emit_clause, query_sql)
            .await?;
        self.reject_stateful_consumer_of_running_state(
            "materialized view",
            &name_str,
            query_sql,
            &planned,
        )
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
        self.validate_cluster_query_shape("materialized view", &name_str, query_sql, &planned)
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
        (planned, interval_topology): (PlannedStreamingQuery, IntervalTopologyCandidate),
        query_sql: &str,
        retention: Option<&MaterializedViewRetention>,
    ) -> Result<(), DbError> {
        let candidate = planned.candidate_registration(name_str, query_sql);
        let PlannedStreamingQuery {
            emit_clause: plan_emit,
            window_config: plan_window,
            order_config: plan_order,
            join_config: plan_joins,
            has_analytic: plan_has_analytic,
            has_frame: plan_has_frame,
            subscription_output: plan_subscription_output,
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

        let (incremental, mode, subscription_output) = self
            .plan_view_store(
                candidate,
                &schema,
                (plan_window.is_some(), &interval_topology),
                retention,
                plan_subscription_output,
            )
            .await?;

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
                // Certifies the view stream's distribution; subscribing to a view stays closed.
                subscription_output,
                subscription_retention_bytes: 0,
                catalog_generation: 1,
                subscription_certificate: None,
            });
            mgr.store_ddl(name_str, sql);
        }

        self.register_mv_provider(name_str, &schema, mode)?;

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
    pub(crate) fn static_table_names(&self) -> rustc_hash::FxHashSet<String> {
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

    /// A non-windowed aggregate, a projection or enrichment of a changelog, and a mutable
    /// interval join maintain a changelog; everything else appends.
    async fn view_output(
        &self,
        query_sql: &str,
        has_window: bool,
        topology: &IntervalTopologyCandidate,
    ) -> ViewOutput {
        if topology.mutable_join {
            return ViewOutput::Changelog { aggregate: false };
        }
        if has_window {
            return ViewOutput::Append;
        }
        let Some(df) = self.ctx.sql(query_sql).await.ok() else {
            return ViewOutput::Append;
        };
        let plan = df.logical_plan();
        if crate::aggregate_state::find_aggregate(plan).is_some() {
            return ViewOutput::Changelog { aggregate: true };
        }
        let enriches_changelog = matches!(
            topology.changelog_input,
            Some(ChangelogInputKind::StaticEnrich)
        ) || crate::sql_analysis::detect_changelog_enrich_query(
            query_sql,
            &self.incremental_mv_names(),
            &self.static_table_names(),
        )
        .is_some();
        if topology.candidate_carries_changelog
            && (crate::sql_analysis::extract_projection_filter(plan).is_some()
                || enriches_changelog)
        {
            return ViewOutput::Changelog { aggregate: false };
        }
        ViewOutput::Append
    }

    /// Decide the store once so the operator and the store agree: a changelog view's stream
    /// emits weighted deltas, its slices follow the planned output distribution, and its
    /// certificate is planned as a changelog, whatever the statement's EMIT says. Returns
    /// whether the view is a changelog, its store and its certificate.
    async fn plan_view_store(
        &self,
        candidate: crate::connector_manager::StreamRegistration,
        schema: &Schema,
        (windowed, interval_topology): (bool, &IntervalTopologyCandidate),
        retention: Option<&MaterializedViewRetention>,
        planned_output: Option<crate::subscription::distribution::PlannedSubscriptionOutput>,
    ) -> Result<
        (
            bool,
            crate::mv_store::MvStorageMode,
            Option<crate::subscription::distribution::PlannedSubscriptionOutput>,
        ),
        DbError,
    > {
        let name = candidate.name.clone();
        let output = self
            .view_output(&candidate.query_sql, windowed, interval_topology)
            .await;
        let incremental = matches!(output, ViewOutput::Changelog { .. });
        let subscription_output = if incremental {
            crate::subscription::distribution::plan(
                &candidate.query_sql,
                Some(&laminar_sql::parser::EmitClause::Changes),
                &self.ctx,
                self.checkpoint_key_groups(),
            )?
        } else {
            self.admit_cluster_view(&name, None)?;
            planned_output
        };
        let mode = self
            .view_storage_mode(candidate, schema, output, retention)
            .await?;
        self.admit_cluster_view(&name, Some(&mode))?;
        Ok((incremental, mode, subscription_output))
    }

    /// The store a view keeps: changelog slices placed by its planned key, or an append log
    /// under its retention.
    async fn view_storage_mode(
        &self,
        candidate: crate::connector_manager::StreamRegistration,
        schema: &Schema,
        output: ViewOutput,
        retention: Option<&MaterializedViewRetention>,
    ) -> Result<crate::mv_store::MvStorageMode, DbError> {
        Ok(match output {
            ViewOutput::Changelog { .. } if retention.is_some() => {
                return Err(DbError::MaterializedView(format!(
                    "materialized view '{}' maintains a changelog; retention applies only to \
                     append-only views",
                    candidate.name
                )));
            }
            ViewOutput::Changelog { aggregate } => crate::mv_store::MvStorageMode::Changelog {
                key: self.planned_view_key(candidate, schema, aggregate).await?,
                key_groups: self.checkpoint_key_groups(),
            },
            ViewOutput::Append => crate::mv_store::MvStorageMode::AppendLog {
                retention: self
                    .append_view_retention(candidate, schema, retention)
                    .await?,
            },
        })
    }

    /// A cluster keeps a changelog view's slices on the nodes that produce them, so it admits
    /// views placed by their producer's key or on one vnode. Append logs (`None` before their
    /// retention resolves) and views placed by the full row need cluster writers and a
    /// repartition port first.
    fn admit_cluster_view(
        &self,
        name: &str,
        mode: Option<&crate::mv_store::MvStorageMode>,
    ) -> Result<(), DbError> {
        use crate::mv_store::{MvStorageMode, ViewKey};

        if !self.is_cluster_runtime() {
            return Ok(());
        }
        let reason = match mode {
            Some(MvStorageMode::Changelog {
                key: ViewKey::Global | ViewKey::Keyed(_) | ViewKey::MultisetByKey(_),
                ..
            }) => return Ok(()),
            Some(MvStorageMode::Changelog {
                key: ViewKey::MultisetByRow,
                ..
            }) => {
                "a view placed by its full row needs a repartition port to the nodes that own \
                 its rows, which has not shipped"
            }
            None | Some(MvStorageMode::AppendLog { .. }) => {
                "append-only views are not admitted in a cluster yet"
            }
        };
        Err(Self::cluster_state_lifecycle_error(
            "materialized view",
            name,
            reason,
        ))
    }

    /// The view key from the view stream's planned output distribution.
    async fn planned_view_key(
        &self,
        candidate: crate::connector_manager::StreamRegistration,
        schema: &Schema,
        aggregate: bool,
    ) -> Result<crate::mv_store::ViewKey, DbError> {
        use crate::exchange_plan::{Distribution, KeyColumn};
        use crate::mv_store::ViewKey;

        let name = candidate.name.clone();
        let resolved = self
            .resolve_catalog_stream_outputs(Some(crate::connector_manager::StreamRegistration {
                incremental: true,
                ..candidate
            }))
            .await?;
        let distribution = &resolved
            .exchange_plan
            .stream(&name)
            .ok_or_else(|| {
                DbError::MaterializedView(format!(
                    "materialized view '{name}' has no exchange plan"
                ))
            })?
            .output
            .distribution;
        let key = match distribution {
            Distribution::Singleton { .. } => return Ok(ViewKey::Global),
            Distribution::Arbitrary => return Ok(ViewKey::MultisetByRow),
            Distribution::Hash(key) => key,
        };
        let columns = key
            .0
            .iter()
            .map(|column| match column {
                KeyColumn::Column { name: column, .. } => schema.index_of(column).ok(),
                KeyColumn::Computed { .. } => None,
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DbError::MaterializedView(format!(
                    "materialized view '{name}' output key {key} is not a set of its columns"
                ))
            })?;
        Ok(if aggregate {
            ViewKey::Keyed(columns)
        } else {
            ViewKey::MultisetByKey(columns)
        })
    }

    fn register_mv_provider(
        &self,
        name_str: &str,
        schema: &Arc<Schema>,
        mode: crate::mv_store::MvStorageMode,
    ) -> Result<(), DbError> {
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
