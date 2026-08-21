//! Temporal and interval topology admission for streams and materialized
//! views: candidate classification, offline-lifecycle gates, and interval
//! output-shape rejections shared by the DDL statement families.

use crate::db::{DbState, LaminarDB};
use crate::error::DbError;

use super::cluster_checks::{ChangelogInputKind, IntervalTopologyCandidate};
use super::drop::CatalogDropTarget;
use super::stream::PlannedStreamingQuery;

impl LaminarDB {
    pub(super) async fn validate_temporal_topology_candidate(
        &self,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<Option<arrow_schema::SchemaRef>, DbError> {
        use laminar_sql::translator::JoinOperatorConfig;

        let joins = plan.join_config.as_deref().unwrap_or_default();
        let contains_temporal = joins
            .iter()
            .any(|join| matches!(join, JoinOperatorConfig::Temporal(_)));
        if !contains_temporal && crate::sql_analysis::has_temporal_query(query_sql) {
            return Err(DbError::Unsupported(
                "the planner did not bind the managed temporal-join contract".into(),
            ));
        }
        let (source_regs, sink_regs, mut stream_regs) = {
            let manager = self.connector_manager.lock();
            (
                manager.sources().clone(),
                manager.sinks().clone(),
                manager.streams().clone(),
            )
        };
        stream_regs.insert(
            name.to_string(),
            crate::connector_manager::StreamRegistration {
                name: name.to_string(),
                query_sql: query_sql.to_string(),
                emit_clause: plan.emit_clause.clone(),
                window_config: plan.window_config.clone(),
                order_config: plan.order_config.clone(),
                join_config: plan.join_config.clone(),
                has_analytic: plan.has_analytic,
                has_frame: plan.has_frame,
                incremental: false,
            },
        );
        self.validate_persisted_temporal_source_contracts(
            &source_regs,
            &sink_regs,
            &stream_regs,
            self.runtime_mode(),
        )?;
        if !contains_temporal {
            return Ok(None);
        }
        let [JoinOperatorConfig::Temporal(config)] = joins else {
            return Err(DbError::Unsupported(
                "managed temporal streams require exactly one two-input temporal join".into(),
            ));
        };
        self.ensure_temporal_stream_offline(name)?;
        let (left, right) = self.validate_temporal_source_metadata(name, config, &source_regs)?;
        crate::pipeline_lifecycle::plan_temporal_output_schema(
            &self.ctx,
            name,
            query_sql,
            config,
            &left.schema,
            &right.schema,
        )
        .await
        .map(Some)
    }

    pub(super) fn ensure_temporal_stream_offline(&self, name: &str) -> Result<(), DbError> {
        if crate::db::catalog_manifest_replay_active()
            || DbState::load(&self.state) == DbState::Created
        {
            return Ok(());
        }
        Err(DbError::Pipeline(format!(
            "[LDB-6043] CREATE STREAM '{name}' contains a temporal join and must be created \
             while the pipeline is stopped so its managed projection and vnode state are \
             initialized before source intake"
        )))
    }

    pub(super) fn reject_interval_output_subquery(
        name: &str,
        query_sql: &str,
    ) -> Result<(), DbError> {
        if crate::sql_analysis::detect_stream_join_query(query_sql).is_some()
            && crate::sql_analysis::interval_output_has_nested_query(query_sql)
        {
            return Err(DbError::InvalidOperation(format!(
                "interval join '{name}' cannot contain a projection or filter subquery"
            )));
        }
        Ok(())
    }

    pub(super) fn reject_temporal_materialized_view(
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<(), DbError> {
        let planned_temporal = plan.join_config.as_ref().is_some_and(|joins| {
            joins.iter().any(|join| {
                matches!(
                    join,
                    laminar_sql::translator::JoinOperatorConfig::Temporal(_)
                )
            })
        });
        if planned_temporal || crate::sql_analysis::has_temporal_query(query_sql) {
            return Err(DbError::Unsupported(
                "materialized views cannot directly own managed temporal-join state; create a temporal stream while the pipeline is stopped"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn validate_interval_topology_candidate(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<IntervalTopologyCandidate, DbError> {
        let (source_regs, sink_regs, mut stream_regs) = {
            let manager = self.connector_manager.lock();
            (
                manager.sources().clone(),
                manager.sinks().clone(),
                manager.streams().clone(),
            )
        };
        stream_regs.insert(
            name.to_string(),
            crate::connector_manager::StreamRegistration {
                name: name.to_string(),
                query_sql: query_sql.to_string(),
                emit_clause: plan.emit_clause.clone(),
                window_config: plan.window_config.clone(),
                order_config: plan.order_config.clone(),
                join_config: plan.join_config.clone(),
                has_analytic: plan.has_analytic,
                has_frame: plan.has_frame,
                incremental: false,
            },
        );
        let temporal_source_roles = self.validate_persisted_temporal_source_contracts(
            &source_regs,
            &sink_regs,
            &stream_regs,
            self.runtime_mode(),
        )?;
        let admissions = self
            .validate_persisted_interval_source_contracts(
                &source_regs,
                &sink_regs,
                &stream_regs,
                self.runtime_mode(),
            )
            .await?;
        let query_references = crate::sql_analysis::extract_table_references(query_sql);
        for input in &query_references {
            self.validate_registered_mutation_source_admission(
                input,
                &source_regs,
                &temporal_source_roles,
                &admissions,
            )?;
        }
        let mutable_interval = admissions.joins.contains_key(name);
        if crate::sql_analysis::query_references_weight(query_sql) {
            return Err(DbError::InvalidOperation(format!(
                "CREATE {object_kind} '{name}' is not a certified changelog producer and cannot explicitly reference or alias the reserved engine-owned '{}' column",
                crate::aggregate_state::WEIGHT_COLUMN
            )));
        }
        let has_existing_changelog_root = !admissions.joins.is_empty()
            || stream_regs.values().any(|registration| {
                registration.incremental
                    || registration.emit_clause.as_ref().is_some_and(|emit| {
                        matches!(emit, laminar_sql::parser::EmitClause::Changes)
                    })
            });
        if !has_existing_changelog_root {
            return Ok(IntervalTopologyCandidate {
                mutable_join: mutable_interval,
                ..IntervalTopologyCandidate::default()
            });
        }
        let reference_tables = self.static_table_names();
        let resolved = crate::pipeline_lifecycle::resolve_stream_output_schemas(
            &self.ctx,
            &stream_regs,
            &reference_tables,
            &admissions.joins,
        )
        .await?;
        let consumes_ordered_changelog = query_references
            .iter()
            .any(|input| resolved.changelog_carrying.contains(input));
        if consumes_ordered_changelog
            && (plan.order_config.is_some()
                || crate::sql_analysis::query_has_order_or_row_limit(query_sql))
        {
            return Err(DbError::InvalidOperation(format!(
                "CREATE {object_kind} '{name}' cannot apply ordering or row limits to a changelog"
            )));
        }
        let candidate_carries_changelog = resolved.changelog_carrying.contains(name);
        let fixed_ordered_topology = mutable_interval || consumes_ordered_changelog;
        if fixed_ordered_topology
            && !crate::db::catalog_manifest_replay_active()
            && DbState::load(&self.state) != DbState::Created
        {
            return Err(DbError::Pipeline(format!(
                "[LDB-6043] CREATE {object_kind} '{name}' creates or consumes a mutable bounded interval changelog and must be created while the pipeline is stopped so its ordered routing and retained state are initialized before source intake"
            )));
        }
        let changelog_enrich = consumes_ordered_changelog
            && crate::sql_analysis::detect_changelog_enrich_query(
                query_sql,
                &resolved.changelog_carrying,
                &reference_tables,
            )
            .is_some();
        Ok(IntervalTopologyCandidate {
            mutable_join: mutable_interval,
            candidate_carries_changelog,
            changelog_input: consumes_ordered_changelog.then_some(if changelog_enrich {
                ChangelogInputKind::StaticEnrich
            } else {
                ChangelogInputKind::Forwarded
            }),
        })
    }

    pub(super) async fn ensure_mutable_interval_drop_offline(
        &self,
        operation: &str,
        targets: &[CatalogDropTarget],
    ) -> Result<(), DbError> {
        if crate::db::catalog_manifest_replay_active()
            || DbState::load(&self.state) != DbState::Running
        {
            return Ok(());
        }
        let (source_regs, sink_regs, stream_regs) = {
            let manager = self.connector_manager.lock();
            (
                manager.sources().clone(),
                manager.sinks().clone(),
                manager.streams().clone(),
            )
        };
        let admissions = self
            .validate_persisted_interval_source_contracts(
                &source_regs,
                &sink_regs,
                &stream_regs,
                self.runtime_mode(),
            )
            .await?;
        if let Some(target) = targets
            .iter()
            .find(|target| admissions.joins.contains_key(&target.name))
        {
            return Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} would remove mutable bounded interval join '{}' from a running fixed startup topology; stop the pipeline first",
                target.name
            )));
        }
        Ok(())
    }
}
