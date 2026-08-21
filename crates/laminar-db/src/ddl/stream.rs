//! Continuous-query stream statements: managed-aggregate admission helpers and
//! `CREATE STREAM` orchestration over the catalog and the coordinator control path.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::StreamingStatement;

use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};
use crate::pipeline::{ControlMutation, ControlMutationState};

use super::catalog::{reject_reserved_namespace, CatalogNameReservation};
use super::control::resolve_control_ack;

struct StreamCreateGuard<'a> {
    db: &'a LaminarDB,
    name: String,
    mutation: Arc<ControlMutation>,
}

impl Drop for StreamCreateGuard<'_> {
    fn drop(&mut self) {
        if self.mutation.cancel() != ControlMutationState::Applied {
            self.db.rollback_catalog_create_or_fence(
                &self.name,
                CatalogObjectKind::Stream,
                "stream create rollback",
            );
        }
    }
}

pub(super) fn contains_builtin_join_without_cluster_lifecycle(
    plan: &Arc<dyn ExecutionPlan>,
) -> bool {
    use datafusion::physical_plan::joins::{
        CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
        SymmetricHashJoinExec,
    };

    plan.is::<CrossJoinExec>()
        || plan.is::<HashJoinExec>()
        || plan.is::<NestedLoopJoinExec>()
        || plan.is::<PiecewiseMergeJoinExec>()
        || plan.is::<SortMergeJoinExec>()
        || plan.is::<SymmetricHashJoinExec>()
        || plan
            .children()
            .into_iter()
            .any(contains_builtin_join_without_cluster_lifecycle)
}

pub(crate) fn logical_aggregate_stage_count(plan: &datafusion_expr::LogicalPlan) -> usize {
    usize::from(matches!(plan, datafusion_expr::LogicalPlan::Aggregate(_)))
        + plan
            .inputs()
            .into_iter()
            .map(logical_aggregate_stage_count)
            .sum::<usize>()
}

pub(crate) struct PlannedStreamingQuery {
    pub(crate) emit_clause: Option<laminar_sql::parser::EmitClause>,
    pub(crate) window_config: Option<laminar_sql::translator::WindowOperatorConfig>,
    pub(crate) order_config: Option<laminar_sql::translator::OrderOperatorConfig>,
    pub(crate) join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
    pub(crate) has_analytic: bool,
    pub(crate) has_frame: bool,
}

pub(crate) async fn validate_managed_aggregate_admission(
    ctx: &datafusion::prelude::SessionContext,
    query_sql: &str,
    window_config: Option<&laminar_sql::translator::WindowOperatorConfig>,
    emit_clause: Option<&laminar_sql::parser::EmitClause>,
    key_group_count: laminar_core::state::KeyGroupCount,
) -> Result<bool, DbError> {
    if let Some(window) = window_config {
        let state = crate::core_window_state::CoreWindowState::try_from_sql(
            ctx,
            query_sql,
            window,
            emit_clause,
            key_group_count,
        )
        .await?;
        if state.is_none() {
            return Err(DbError::InvalidOperation(
                "window aggregate cannot be constructed on the managed execution path".into(),
            ));
        }
        return Ok(true);
    }

    let emit_changelog =
        emit_clause.is_some_and(|emit| matches!(emit, laminar_sql::parser::EmitClause::Changes));
    crate::aggregate_state::IncrementalAggState::try_from_sql(
        ctx,
        query_sql,
        emit_changelog,
        key_group_count,
    )
    .await
    .map(|state| state.is_some())
}

pub(super) fn query_inputs_registered(
    ctx: &datafusion::prelude::SessionContext,
    query_sql: &str,
) -> bool {
    crate::sql_analysis::extract_table_references(query_sql)
        .iter()
        .all(|name| {
            ctx.table_exist(exact_table_reference(name))
                .unwrap_or(false)
        })
}

impl LaminarDB {
    pub(crate) async fn handle_create_stream(
        &self,
        sql: &str,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
        if_not_exists: bool,
        query_sql: &str,
        retention_bytes: Option<u64>,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("CREATE STREAM")?;
        let name_str = canonical_object_name(name)?;
        reject_reserved_namespace(&name_str)?;
        if crate::sql_analysis::has_temporal_query(query_sql) {
            self.ensure_temporal_stream_offline(&name_str)?;
        }
        Self::reject_interval_output_subquery(&name_str, query_sql)?;
        let Some(reservation) =
            self.reserve_catalog_name(&name_str, CatalogObjectKind::Stream, if_not_exists)?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE STREAM".to_string(),
                object_name: name_str,
                applied: false,
            }));
        };

        let (planned, temporal_output_schema) = self
            .validate_stream_admission("stream", name, query, emit_clause, query_sql)
            .await?;
        self.register_and_admit_stream(
            sql,
            &name_str,
            reservation,
            planned,
            temporal_output_schema,
            query_sql,
            retention_bytes,
        )
        .await?;

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE STREAM".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    /// Every pre-mutation stream admission check, returning the planned query
    /// and the temporal-output schema when the query is a managed temporal join.
    #[allow(clippy::type_complexity)]
    async fn validate_stream_admission(
        &self,
        kind: &str,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
        query_sql: &str,
    ) -> Result<(PlannedStreamingQuery, Option<arrow::datatypes::SchemaRef>), DbError> {
        let name_str = canonical_object_name(name)?;
        self.validate_cluster_query_shape_before_plan(kind, &name_str, query_sql)?;
        let planned = self
            .plan_streaming_query(name, query, emit_clause.cloned(), query_sql)
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
        let temporal_output_schema = self
            .validate_temporal_topology_candidate(&name_str, query_sql, &planned)
            .await?;
        self.validate_interval_join_schema(&name_str, query_sql, &planned)
            .await?;
        let _ = self
            .validate_interval_topology_candidate(kind, &name_str, query_sql, &planned)
            .await?;
        self.validate_cluster_query_shape(kind, &name_str, query_sql, &planned)
            .await?;

        // A stream over an incremental MV must net the changelog — a non-windowed aggregate or a
        // simple projection/filter; a complex shape (e.g. a join) is rejected.
        self.reject_unsupported_reading_incremental_mv(
            query_sql,
            "a stream",
            planned.window_config.is_some(),
        )
        .await?;
        Ok((planned, temporal_output_schema))
    }

    /// Catalog registration, placeholder provider, and the coordinator control
    /// admission round. The create guard lives here so its rollback fence
    /// covers exactly this phase; `reservation` commits only after the
    /// admission acknowledgement resolves.
    async fn register_and_admit_stream(
        &self,
        sql: &str,
        name_str: &str,
        mut reservation: CatalogNameReservation<'_>,
        planned: PlannedStreamingQuery,
        temporal_output_schema: Option<arrow::datatypes::SchemaRef>,
        query_sql: &str,
        retention_bytes: Option<u64>,
    ) -> Result<(), DbError> {
        let PlannedStreamingQuery {
            emit_clause: plan_emit,
            window_config: plan_window,
            order_config: plan_order,
            join_config: plan_joins,
            has_analytic: plan_has_analytic,
            has_frame: plan_has_frame,
        } = planned;
        let query_sql = query_sql.to_string();

        // The typed namespace reservation prevents rollback from erasing another object.
        self.catalog.register_stream(name_str)?;
        let mutation = Arc::new(ControlMutation::new());
        reservation.bind_control_mutation(Arc::clone(&mutation));
        let _create_guard = StreamCreateGuard {
            db: self,
            name: name_str.to_string(),
            mutation: Arc::clone(&mutation),
        };

        #[cfg(test)]
        let topology_planning_gate = { self.topology_planning_gate.lock().clone() };
        #[cfg(test)]
        if let Some((entered, release)) = topology_planning_gate {
            entered.notify_one();
            release.notified().await;
        }

        let placeholder_schema = match temporal_output_schema {
            Some(schema) => Some(schema),
            None => crate::pipeline_lifecycle::plan_output_schema(&self.ctx, &query_sql).await,
        };

        if let Some(bytes) = retention_bytes {
            let cap = usize::try_from(bytes).unwrap_or(usize::MAX);
            self.subscription_registry.configure(name_str, cap);
        }

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
                incremental: false,
            });
            // Local replay identity participates in the same cancellation guard as
            // graph/catalog admission. Once the coordinator CAS is Applied, caller
            // cancellation must not leave an unreplayable live topology.
            mgr.store_ddl(name_str, sql);
        }

        // Register as a DataFusion placeholder for plan-time name resolution by downstream MVs.
        if let Some(schema) = placeholder_schema {
            use datafusion::datasource::empty::EmptyTable;
            if let Err(e) = self.ctx.register_table(
                exact_table_reference(name_str),
                Arc::new(EmptyTable::new(schema)),
            ) {
                return Err(DbError::Pipeline(format!(
                    "could not register stream '{name_str}' for downstream planning: {e}"
                )));
            }
        }

        // Hot-add is acknowledged only after graph admission and wiring complete.
        // The oneshot closes if the pipeline exits, so a rejected/stopped runtime rolls DDL back.
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
                    false,
                    reply,
                    Arc::clone(&mutation),
                ))
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "control channel busy, retry CREATE STREAM '{name_str}': {e}"
                    ))
                })?;
                Ok::<_, DbError>(admission)
            })
        }
        .transpose();
        let admission_result = match admission {
            Ok(Some(admission)) => {
                resolve_control_ack(&format!("CREATE STREAM '{name_str}'"), admission, &mutation)
                    .await
            }
            Ok(None) => {
                self.apply_without_live_control(&format!("CREATE STREAM '{name_str}'"), &mutation)
            }
            Err(error) => Err(error),
        };
        admission_result?;

        reservation.commit();
        Ok(())
    }
    pub(super) async fn plan_streaming_query(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<laminar_sql::parser::EmitClause>,
        query_sql: &str,
    ) -> Result<PlannedStreamingQuery, DbError> {
        let admission = if crate::sql_analysis::has_join_clause(query_sql) {
            let (source_regs, sink_regs, stream_regs) = {
                let manager = self.connector_manager.lock();
                (
                    manager.sources().clone(),
                    manager.sinks().clone(),
                    manager.streams().clone(),
                )
            };
            let ordered = self
                .validate_persisted_interval_source_contracts(
                    &source_regs,
                    &sink_regs,
                    &stream_regs,
                    self.runtime_mode(),
                )
                .await?;
            let static_tables = self.static_table_names();
            let current = crate::pipeline_lifecycle::resolve_stream_output_schemas(
                &self.ctx,
                &stream_regs,
                &static_tables,
                &ordered.joins,
            )
            .await?;
            if let Some(join) = crate::sql_analysis::detect_changelog_enrich_query(
                query_sql,
                &current.changelog_carrying,
                &static_tables,
            ) {
                Some(
                    laminar_sql::planner::ChangelogEnrichAdmission::try_new(
                        join.changelog_table,
                        join.static_table,
                        join.left_keys,
                        join.right_keys,
                        join.left_outer,
                    )
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "invalid dimension-join admission certificate: {error}"
                        ))
                    })?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let mut planner = self.planner.lock();
        let statement = StreamingStatement::CreateStream {
            name: name.clone(),
            query: Box::new(query.clone()),
            emit_clause,
            or_replace: false,
            if_not_exists: false,
            query_sql: query_sql.to_string(),
            retention_bytes: None,
        };
        let plan_result = if let Some(admission) = admission.as_ref() {
            planner.plan_changelog_enrich(&statement, admission)
        } else {
            planner.plan(&statement)
        };
        let laminar_sql::planner::StreamingPlan::Query(plan) =
            plan_result.map_err(laminar_sql::Error::from)?
        else {
            return Err(DbError::InvalidOperation(format!(
                "planner did not produce a streaming query for '{name}'"
            )));
        };
        Ok(PlannedStreamingQuery {
            emit_clause: plan.emit_clause,
            window_config: plan.window_config,
            order_config: plan.order_config,
            join_config: plan.join_config,
            has_analytic: plan.analytic_config.is_some(),
            has_frame: plan.frame_config.is_some(),
        })
    }
}
