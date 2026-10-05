//! Single-node database construction before catalog and worker startup.

use std::sync::Arc;

use laminar_db::{EngineMetrics, LaminarDB};

use crate::config::ServerConfig;

use super::{apply_local_checkpoint_config, ServerError};

pub(super) fn install_metrics(
    db: &LaminarDB,
    config: &ServerConfig,
) -> Result<Arc<prometheus::Registry>, ServerError> {
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let pipeline_name = config
        .pipelines
        .first()
        .map_or("default", |p| p.name.as_str())
        .to_string();
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), hostname),
        ("pipeline".into(), pipeline_name),
    ]));
    db.set_engine_metrics(Arc::new(EngineMetrics::new(&registry)));
    db.set_prometheus_registry(Arc::clone(&registry))
        .map_err(|error| ServerError::Start(error.to_string()))?;
    Ok(registry)
}

pub(super) async fn build(config: &ServerConfig) -> Result<Arc<LaminarDB>, ServerError> {
    let mut builder = LaminarDB::builder();
    builder = builder.delivery_guarantee(config.server.delivery);
    if let Some(ref token) = config.server.console_token {
        builder = builder.http_auth_token(token.expose());
    }
    builder = builder.restart_policy(config.supervision.to_policy());
    builder = builder.incremental_emit(config.server.incremental_emit);
    builder = config.server.apply_memory_limits(builder);
    if let Some(retention) = config.server.temporal_join_idle_history_retention {
        builder = builder.temporal_join_idle_history_retention(retention);
    }
    if let Some(timeout) = config.server.source_idle_timeout {
        builder = builder.source_idle_timeout(timeout);
    }
    builder = builder.event_time_max_future_skew(config.server.event_time_max_future_skew);
    builder = apply_local_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint)
        .map_err(|error| ServerError::Build(format!("checkpoint storage: {error}")))?;

    let key_groups = config.server.resolved_key_groups();
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(
        u32::from(key_groups),
        laminar_core::state::LOCAL_NODE_ID,
    ));
    builder = builder.vnode_registry(vnode_registry);

    if let Some(ai_runtime) = crate::ai::build_ai_runtime(config)? {
        builder = builder.ai(ai_runtime);
    }

    let db = builder
        .build()
        .await
        .map_err(|error| ServerError::Build(error.to_string()))?;
    // A failed local worker has no in-place replacement. A generic graph restart would reuse
    // the dead client and could reopen source intake.
    if config.process_functions.is_empty() {
        db.enable_supervision();
    }
    Ok(db)
}
