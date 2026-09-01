//! Cluster-specific DDL checks: temporal/interval topology candidate admission,
//! interval join schema certification, and cluster query-shape validation.
//!
//! These gates are fail-closed by design — cluster admission is deliberately
//! narrower than embedded and must never be widened to make a test pass.

use arrow::datatypes::{DataType, Schema};

use crate::db::{exact_table_reference, LaminarDB};
use crate::error::DbError;

use super::stream::{
    contains_builtin_join_without_cluster_lifecycle, logical_aggregate_stage_count,
    PlannedStreamingQuery,
};

/// Schema field lookup used by the interval-join checks.
type IntervalFieldLookup<'a> = &'a dyn Fn(&Schema, &str, &str) -> Result<(DataType, bool), DbError>;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct IntervalTopologyCandidate {
    pub(super) mutable_join: bool,
    pub(super) candidate_carries_changelog: bool,
    pub(super) changelog_input: Option<ChangelogInputKind>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ChangelogInputKind {
    Forwarded,
    StaticEnrich,
}

impl LaminarDB {
    pub(crate) async fn validate_interval_join_schema(
        &self,
        object_name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<(), DbError> {
        let Some(joins) = &plan.join_config else {
            return Ok(());
        };

        let mut has_interval_join = false;
        for join in joins {
            let laminar_sql::translator::JoinOperatorConfig::StreamStream(config) = join else {
                continue;
            };
            has_interval_join = true;
            self.validate_one_interval_join(object_name, config).await?;
        }

        if has_interval_join {
            if crate::sql_analysis::has_unaliased_projection(query_sql) {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' requires every projected expression to have an explicit alias"
                )));
            }
            if crate::sql_analysis::has_unqualified_interval_output_column(query_sql) {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' requires every projected or filtered column to use its left or right input qualifier"
                )));
            }
            let dataframe = self.ctx.sql(query_sql).await.map_err(|error| {
                DbError::InvalidOperation(format!(
                    "interval join '{object_name}' could not be validated: {error}"
                ))
            })?;
            if logical_aggregate_stage_count(dataframe.logical_plan()) != 0 {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' cannot contain an aggregate stage"
                )));
            }
        }
        Ok(())
    }

    /// Certify one interval join against its registered input schemas,
    /// equality keys, event-time columns, and source watermark contracts.
    async fn validate_one_interval_join(
        &self,
        object_name: &str,
        config: &laminar_sql::translator::StreamJoinConfig,
    ) -> Result<(), DbError> {
        let left_schema = self
            .ctx
            .table_provider(exact_table_reference(&config.left_table))
            .await
            .map_err(|error| {
                DbError::InvalidOperation(format!(
                    "cannot validate interval join '{object_name}' input '{}': {error}",
                    config.left_table
                ))
            })?
            .schema();
        let right_schema = self
            .ctx
            .table_provider(exact_table_reference(&config.right_table))
            .await
            .map_err(|error| {
                DbError::InvalidOperation(format!(
                    "cannot validate interval join '{object_name}' input '{}': {error}",
                    config.right_table
                ))
            })?
            .schema();

        let field = |schema: &Schema, table: &str, column: &str| {
            schema
                .field_with_name(column)
                .map(|field| (field.data_type().clone(), field.is_nullable()))
                .map_err(|_| {
                    DbError::InvalidOperation(format!(
                        "interval join '{object_name}' column '{table}.{column}' does not exist"
                    ))
                })
        };

        Self::validate_interval_join_keys(
            object_name,
            config,
            &left_schema,
            &right_schema,
            &field,
        )?;
        Self::validate_interval_join_event_time(
            object_name,
            config,
            &left_schema,
            &right_schema,
            &field,
        )?;
        Self::validate_interval_join_watermarks(self, config)
    }

    /// Certify one interval join's equality keys and internal output-name
    /// uniqueness for the supported join kinds.
    fn validate_interval_join_keys(
        object_name: &str,
        config: &laminar_sql::translator::StreamJoinConfig,
        left_schema: &Schema,
        right_schema: &Schema,
        field: IntervalFieldLookup<'_>,
    ) -> Result<(), DbError> {
        if config.left_keys.is_empty() || config.left_keys.len() != config.right_keys.len() {
            return Err(DbError::InvalidOperation(format!(
                "interval join '{object_name}' requires non-empty equality-key vectors with matching arity"
            )));
        }
        for (left_column, right_column) in config.left_keys.iter().zip(config.right_keys.iter()) {
            let (left_key, _) = field(left_schema, &config.left_table, left_column)?;
            let (right_key, _) = field(right_schema, &config.right_table, right_column)?;
            if !matches!(&left_key, DataType::Utf8 | DataType::Int64)
                || !matches!(&right_key, DataType::Utf8 | DataType::Int64)
                || left_key != right_key
            {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' key pairs must have the same Utf8 or Int64 type; '{}.{}' is {} and '{}.{}' is {}",
                    config.left_table,
                    left_column,
                    left_key,
                    config.right_table,
                    right_column,
                    right_key
                )));
            }
        }

        if matches!(
            config.join_type,
            laminar_sql::parser::join_parser::JoinType::Inner
                | laminar_sql::parser::join_parser::JoinType::Left
                | laminar_sql::parser::join_parser::JoinType::Right
                | laminar_sql::parser::join_parser::JoinType::Full
        ) {
            let mut output_names = std::collections::HashSet::new();
            for field in left_schema.fields() {
                if !output_names.insert(field.name().clone()) {
                    return Err(DbError::InvalidOperation(format!(
                        "interval join '{object_name}' input schema repeats column '{}'",
                        field.name()
                    )));
                }
            }
            for field in right_schema.fields() {
                let name = format!("{}_{}", field.name(), config.right_table);
                if !output_names.insert(name.clone()) {
                    return Err(DbError::InvalidOperation(format!(
                        "interval join '{object_name}' internal output column '{name}' collides; rename the input column or relation"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Certify the interval join's event-time columns: Timestamp type on both
    /// sides, declared NOT NULL.
    fn validate_interval_join_event_time(
        object_name: &str,
        config: &laminar_sql::translator::StreamJoinConfig,
        left_schema: &Schema,
        right_schema: &Schema,
        field: IntervalFieldLookup<'_>,
    ) -> Result<(), DbError> {
        for (schema, table, column) in [
            (&left_schema, &config.left_table, &config.left_time_column),
            (
                &right_schema,
                &config.right_table,
                &config.right_time_column,
            ),
        ] {
            let (time, nullable) = field(schema, table, column)?;
            if !matches!(&time, DataType::Timestamp(_, _)) {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' event-time column '{table}.{column}' must be Timestamp(_), found {time}"
                )));
            }
            if nullable {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' event-time column '{table}.{column}' must be declared NOT NULL"
                )));
            }
        }
        Ok(())
    }

    /// Certify that both interval-join inputs are directly watermarked,
    /// event-time sources.
    fn validate_interval_join_watermarks(
        db: &LaminarDB,
        config: &laminar_sql::translator::StreamJoinConfig,
    ) -> Result<(), DbError> {
        for (side, source_name, time_column) in [
            (
                "left",
                config.left_table.as_str(),
                config.left_time_column.as_str(),
            ),
            (
                "right",
                config.right_table.as_str(),
                config.right_time_column.as_str(),
            ),
        ] {
            let Some(source) = db.catalog.get_source(source_name) else {
                return Err(DbError::InvalidOperation(format!(
                    "{side} interval join input '{source_name}' must be a directly watermarked source"
                )));
            };
            if source.watermark_column.as_deref() != Some(time_column)
                || source
                    .is_processing_time
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(DbError::InvalidOperation(format!(
                    "{side} interval join input '{source_name}' must define an event-time watermark on '{time_column}'"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn cluster_state_lifecycle_error(
        object_kind: &str,
        name: &str,
        reason: &str,
    ) -> DbError {
        DbError::InvalidOperation(format!(
            "[{}] {object_kind} '{name}' is not supported in cluster mode: {reason}",
            laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED
        ))
    }

    pub(super) fn validate_cluster_query_shape_before_plan(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
    ) -> Result<(), DbError> {
        if !self.is_cluster_runtime() {
            return Ok(());
        }
        let reject = |reason: &str| {
            Err(Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                reason,
            ))
        };

        if crate::sql_analysis::plan_frame_query(query_sql).is_some() {
            return reject(
                "analytic/window-frame state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }
        let hazards = crate::sql_analysis::cluster_query_hazards(query_sql).ok_or_else(|| {
            Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                "query SQL could not be certified by the cluster admission parser",
            )
        })?;
        if hazards.runtime_function {
            return reject(
                "runtime clock/watermark functions require a cluster-wide evaluation frontier",
            );
        }
        if hazards.ai_function {
            return reject(
                "AI inference has checkpointed in-flight rows but no vnode-keyed rebalance lifecycle",
            );
        }
        if crate::sql_analysis::has_join_clause(query_sql)
            && self.first_incremental_ref(query_sql).is_some()
        {
            return reject(
                "incremental changelog join state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }
        Ok(())
    }

    /// Cluster admission is based on configured runtime mode, never the current owner count.
    /// Every stateful route admitted here must implement key shuffle plus vnode capture, restore,
    /// and revoke. Bounded interval joins and managed temporal joins satisfy that contract.
    pub(crate) async fn validate_cluster_query_shape(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<bool, DbError> {
        use laminar_sql::translator::OrderOperatorConfig;

        if !self.is_cluster_runtime() {
            return Ok(false);
        }
        self.validate_cluster_query_shape_before_plan(object_kind, name, query_sql)?;

        if plan.has_analytic || plan.has_frame {
            return Err(Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                "analytic/window-frame state has no vnode-keyed checkpoint and rebalance lifecycle",
            ));
        }
        let managed_window_emit = plan.emit_clause.as_ref().is_some_and(|emit| {
            matches!(
                emit,
                laminar_sql::parser::EmitClause::OnWindowClose
                    | laminar_sql::parser::EmitClause::Final
            )
        });
        if let Some(true) = self
            .validate_cluster_window_shape(object_kind, name, query_sql, plan, managed_window_emit)
            .await?
        {
            return Ok(true);
        }
        if managed_window_emit {
            return Err(Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                "window-close/final emission requires a managed TUMBLE, HOP, or SESSION aggregate",
            ));
        }
        if plan
            .order_config
            .as_ref()
            .is_some_and(|order| !matches!(order, OrderOperatorConfig::SourceSatisfied))
        {
            return Err(Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                "ORDER BY/TOP-K has no distributed merge and vnode-keyed state lifecycle",
            ));
        }
        if let Some(true) = self.validate_cluster_join_shape(object_kind, name, query_sql, plan)? {
            return Ok(true);
        }

        self.validate_cluster_aggregate_shape(object_kind, name, query_sql, plan)
            .await
    }

    /// Managed `TUMBLE`/`HOP`/`SESSION` window admission. `Some(true)` admits the
    /// managed window path; `None` falls through to the join/aggregate routes.
    async fn validate_cluster_window_shape(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
        managed_window_emit: bool,
    ) -> Result<Option<bool>, DbError> {
        let reject = |reason: &str| -> DbError {
            Self::cluster_state_lifecycle_error(object_kind, name, reason)
        };

        let Some(window) = plan.window_config.as_ref() else {
            return Ok(None);
        };
        if !managed_window_emit {
            return Err(reject(
                "cluster windows require EMIT ON WINDOW CLOSE or EMIT FINAL on the managed CoreWindow path",
            ));
        }
        if plan.join_config.is_some() || crate::sql_analysis::has_join_clause(query_sql) {
            return Err(reject(
                "a windowed join requires a planner-certified combined join/window vnode lifecycle",
            ));
        }
        let source_name = crate::sql_analysis::managed_core_window_source(query_sql, window)
            .ok_or_else(|| {
                reject(
                    "managed CoreWindow execution requires one direct source, an unqualified event-time column, and no nested or row-expanding query shape",
                )
            })?;
        let source = self.catalog.get_source(&source_name).ok_or_else(|| {
            reject("managed CoreWindow execution requires exactly one direct source")
        })?;
        if source
            .is_processing_time
            .load(std::sync::atomic::Ordering::Acquire)
            || source.watermark_column.as_deref() != Some(window.time_column.as_str())
            || source.max_out_of_orderness.is_none()
        {
            return Err(reject(
                "managed CoreWindow execution requires an event-time watermark on its window time column",
            ));
        }
        let managed = crate::core_window_state::CoreWindowState::try_from_sql(
            &self.ctx,
            query_sql,
            window,
            plan.emit_clause.as_ref(),
            self.checkpoint_key_groups(),
        )
        .await
        .map_err(|error| reject(&format!("managed CoreWindow validation failed: {error}")))?
        .ok_or_else(|| {
            reject("window aggregate cannot be constructed on the managed CoreWindow path")
        })?;
        if !managed.planned_functions_are_immutable() {
            return Err(reject(
                "managed CoreWindow execution requires replay-immutable planned functions",
            ));
        }
        if managed.compiled_projection().is_none() {
            return Err(reject(
                "managed CoreWindow execution requires compiled pre-aggregation over its direct source",
            ));
        }
        #[cfg(feature = "cluster")]
        if self.shuffle_sender.lock().is_none()
            || self.shuffle_receiver.lock().is_none()
            || self.vnode_registry.lock().is_none()
        {
            return Err(reject(
                "CoreWindow has no complete shuffle and vnode ownership scope",
            ));
        }
        Ok(Some(true))
    }

    /// Join admission: managed temporal joins and certified bounded interval
    /// joins. `Some(true)` admits; `None` falls through to the aggregate route.
    fn validate_cluster_join_shape(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<Option<bool>, DbError> {
        use laminar_sql::translator::JoinOperatorConfig;

        let reject = |reason: &str| -> DbError {
            Self::cluster_state_lifecycle_error(object_kind, name, reason)
        };

        let Some(joins) = &plan.join_config else {
            if crate::sql_analysis::has_temporal_query(query_sql) {
                return Err(reject(
                    "the planner did not bind the managed temporal-join contract",
                ));
            }
            if crate::sql_analysis::detect_stream_join_query(query_sql).is_some() {
                return Err(reject(
                    "the planner did not bind the bounded interval-join contract",
                ));
            }
            return Ok(None);
        };
        let [join] = joins.as_slice() else {
            return Err(reject(
                "cluster streaming joins require exactly one two-input stage",
            ));
        };
        if let JoinOperatorConfig::Temporal(config) = join {
            crate::sql_analysis::temporal_projection_sql(query_sql, config).map_err(|error| {
                reject(&format!("managed temporal join validation failed: {error}"))
            })?;
            #[cfg(feature = "cluster")]
            if self.shuffle_sender.lock().is_none()
                || self.shuffle_receiver.lock().is_none()
                || self.vnode_registry.lock().is_none()
            {
                return Err(reject(
                    "temporal join has no complete shuffle and vnode ownership scope",
                ));
            }
            return Ok(Some(true));
        }
        let JoinOperatorConfig::StreamStream(config) = join else {
            return Err(reject(
                "lookup join operator and output state have no vnode lifecycle",
            ));
        };
        if config.time_bound.is_zero() || i64::try_from(config.time_bound.as_millis()).is_err() {
            return Err(reject(
                "the distributed join supports only certified direct-source bounded equality joins with a positive finite event-time bound",
            ));
        }
        let detected =
            crate::sql_analysis::detect_stream_join_query(query_sql).ok_or_else(|| {
                reject("the planner join does not map to the bounded interval-join execution path")
            })?;
        if detected.config.left_table != config.left_table
            || detected.config.right_table != config.right_table
            || detected.config.join_type != config.join_type
            || detected.config.left_keys != config.left_keys
            || detected.config.right_keys != config.right_keys
            || detected.config.left_time_column != config.left_time_column
            || detected.config.right_time_column != config.right_time_column
            || detected.config.time_bound != config.time_bound
        {
            return Err(reject(
                "planner and interval-join execution metadata disagree",
            ));
        }
        #[cfg(feature = "cluster")]
        if self.shuffle_sender.lock().is_none()
            || self.shuffle_receiver.lock().is_none()
            || self.vnode_registry.lock().is_none()
        {
            return Err(reject(
                "interval join has no complete shuffle and vnode ownership scope",
            ));
        }
        Ok(Some(true))
    }

    /// Projection/filter and single-stage keyed-aggregate admission over the
    /// physical plan. Returns whether an aggregate stage is present.
    async fn validate_cluster_aggregate_shape(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<bool, DbError> {
        let reject = |reason: &str| -> DbError {
            Self::cluster_state_lifecycle_error(object_kind, name, reason)
        };

        let dataframe =
            self.ctx.sql(query_sql).await.map_err(|error| {
                reject(&format!("cluster shape could not be validated: {error}"))
            })?;
        let logical_aggregate_stages = logical_aggregate_stage_count(dataframe.logical_plan());
        let physical = self
            .ctx
            .state()
            .create_physical_plan(dataframe.logical_plan())
            .await
            .map_err(|error| {
                reject(&format!(
                    "cluster physical plan could not be validated: {error}"
                ))
            })?;
        if contains_builtin_join_without_cluster_lifecycle(&physical) {
            return Err(reject(
                "a built-in DataFusion join has no distributed shuffle and vnode state lifecycle",
            ));
        }
        let has_aggregate = match logical_aggregate_stages {
            0 => false,
            1 => true,
            logical => {
                return Err(reject(&format!(
                    "aggregate plan has {logical} logical aggregate stages; cluster admission requires at most one until multi-stage distribution is planner-certified"
                )));
            }
        };
        if has_aggregate {
            #[cfg(feature = "cluster")]
            if self.shuffle_sender.lock().is_none()
                || self.shuffle_receiver.lock().is_none()
                || self.vnode_registry.lock().is_none()
            {
                return Err(reject(
                    "aggregate has no complete distributed shuffle and vnode ownership scope",
                ));
            }
            let emit_changelog = plan
                .emit_clause
                .as_ref()
                .is_some_and(|emit| matches!(emit, laminar_sql::parser::EmitClause::Changes));
            let aggregate = match crate::aggregate_state::IncrementalAggState::try_from_sql(
                &self.ctx,
                query_sql,
                emit_changelog,
                self.checkpoint_key_groups(),
            )
            .await
            {
                Ok(Some(aggregate)) => aggregate,
                Ok(None) => {
                    return Err(reject(
                        "aggregate cannot be constructed on the exact incremental execution path",
                    ));
                }
                Err(error) => {
                    return Err(reject(&format!(
                        "aggregate incremental execution path could not be constructed: {error}"
                    )));
                }
            };
            let certified = plan.subscription_output.as_ref().is_some_and(|output| {
                output.matches_aggregate_grouping(aggregate.num_group_cols())
            });
            if !certified {
                return Err(reject(
                    "aggregate final output has no matching planner-owned subscription distribution certificate",
                ));
            }
        } else if plan.subscription_output.is_some() {
            return Err(reject(
                "non-aggregate plan carried an invalid subscription distribution certificate",
            ));
        }
        Ok(has_aggregate)
    }
}
