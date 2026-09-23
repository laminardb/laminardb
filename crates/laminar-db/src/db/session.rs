use super::{DbError, LaminarConfig, LaminarDB, LookupQueryPlanner};
use datafusion::execution::disk_manager::{DiskManager, DiskManagerMode};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub(super) fn create_context(
    config: &LaminarConfig,
    lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    extra_optimizer_rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
    target_partitions: Option<usize>,
) -> Result<SessionContext, DbError> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(
            config.datafusion_memory_limit_bytes,
        )))
        // Compute cannot perform blocking spill I/O. Share this policy with ad-hoc queries too.
        .with_disk_manager_builder(DiskManager::builder().with_mode(DiskManagerMode::Disabled))
        .build_arc()?;
    let mut session_config = laminar_sql::datafusion::base_session_config();
    if let Some(n) = target_partitions {
        session_config = session_config.with_target_partitions(n);
    }
    let extension_planner = Arc::new(laminar_sql::datafusion::LookupJoinExtensionPlanner::new(
        lookup_registry,
    ));
    let mut state_builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_query_planner(Arc::new(LookupQueryPlanner { extension_planner }));
    for rule in extra_optimizer_rules {
        state_builder = state_builder.with_physical_optimizer_rule(Arc::clone(rule));
    }
    let context = SessionContext::new_with_state(state_builder.build());
    laminar_sql::register_streaming_functions(&context);
    Ok(context)
}

impl LaminarDB {
    pub(crate) fn create_operator_context(&self) -> SessionContext {
        let mut session_config = laminar_sql::datafusion::base_session_config();
        if let Some(n) = self.pipeline_target_partitions {
            session_config = session_config.with_target_partitions(n);
        }
        let state = self.ctx.state();
        let mut state_builder = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(self.ctx.runtime_env())
            .with_default_features()
            .with_optimizer_rules(state.optimizers().to_vec())
            .with_query_planner(Arc::clone(state.query_planner()));
        for rule in self.physical_optimizer_rules.iter() {
            state_builder = state_builder.with_physical_optimizer_rule(Arc::clone(rule));
        }
        let context = SessionContext::new_with_state(state_builder.build());
        laminar_sql::register_streaming_functions(&context);
        self.register_custom_functions_into(&context);
        context
    }

    // Keep temporary filter/diagnostic catalogs separate while retaining the DB's budget.
    pub(crate) fn create_auxiliary_context(&self) -> SessionContext {
        let context = SessionContext::new_with_config_rt(
            laminar_sql::datafusion::base_session_config(),
            self.ctx.runtime_env(),
        );
        laminar_sql::register_streaming_functions(&context);
        self.register_custom_functions_into(&context);
        context
    }
}
