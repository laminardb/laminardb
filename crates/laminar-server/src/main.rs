//! LaminarDB standalone server binary.

#![allow(clippy::disallowed_types)] // cold path: server startup and config only

mod ai;
#[cfg(feature = "cluster")]
mod cluster;
#[cfg(feature = "cluster")]
mod cluster_config;
mod config;
mod http;
mod metrics;
mod pgwire;
mod reload;
mod server;
mod watcher;

// Platform-dependent allocator selection:
// - Unix / non-MSVC: jemalloc (excellent fragmentation control, NUMA-aware)
// - Windows MSVC: mimalloc (only high-perf allocator supporting MSVC)
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", target_env = "msvc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about = "LaminarDB streaming database server")]
struct Args {
    #[arg(short, long, default_value = "laminardb.toml")]
    config: String,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long)]
    admin_bind: Option<String>,
    /// Postgres wire bind address (e.g. `127.0.0.1:5433`). Wildcard binds rejected.
    #[arg(long)]
    pgwire_bind: Option<String>,
    /// Validate checkpoints and exit without starting the server.
    #[arg(long)]
    validate_checkpoints: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // `laminardb` is the BIN crate: main.rs/cluster.rs etc.
                // log under that target, not `laminar_server` (the lib
                // name) — without it the server's own startup logs are
                // silently filtered out.
                format!(
                    "laminardb={l},laminar_server={l},laminar_db={l},laminar_core={l},\
                     laminar_sql={l},laminar_connectors={l}",
                    l = args.log_level
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting LaminarDB server");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("Config file: {}", args.config);

    let config_path = PathBuf::from(&args.config);
    let mut config = config::load_config(&config_path)?;

    if let Some(bind) = args.admin_bind {
        config.server.bind = bind;
    }
    if let Some(pg) = args.pgwire_bind {
        // Empty string disables; a value overrides config.
        config.server.pgwire_bind = if pg.is_empty() { None } else { Some(pg) };
    }

    if args.validate_checkpoints {
        return validate_checkpoints_and_exit(&config).await;
    }

    let handle = server::run_server(config, config_path).await?;
    handle.wait_for_shutdown().await?;

    Ok(())
}

async fn validate_checkpoints_and_exit(config: &config::ServerConfig) -> Result<()> {
    let store = build_checkpoint_store(config)?;

    info!("Validating checkpoints...");
    let report = store
        .recover_latest_validated()
        .await
        .map_err(|e| anyhow::anyhow!("validation failed: {e}"))?;

    info!(
        "Examined {} checkpoint(s) in {:?}",
        report.examined, report.elapsed
    );
    for (id, reason) in &report.skipped {
        info!("  INVALID checkpoint {id}: {reason}");
    }
    match checkpoint_validation_outcome(&report)? {
        CheckpointValidationOutcome::StructurallyValid(id) => {
            info!("  STRUCTURALLY VALID checkpoint {id} selected for recovery");
        }
        CheckpointValidationOutcome::Empty => info!("  No checkpoints found (fresh start)"),
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum CheckpointValidationOutcome {
    Empty,
    StructurallyValid(u64),
}

fn checkpoint_validation_outcome(
    report: &laminar_core::storage::checkpoint_store::RecoveryReport,
) -> Result<CheckpointValidationOutcome> {
    match report.chosen_id {
        Some(id) => Ok(CheckpointValidationOutcome::StructurallyValid(id)),
        None if report.examined == 0 => Ok(CheckpointValidationOutcome::Empty),
        None => Err(anyhow::anyhow!(
            "[LDB-6041] checkpoint history exists but no valid checkpoint is recoverable"
        )),
    }
}

/// Build the checkpoint store for `--validate-checkpoints`. Errors
/// fail the validation run: `checkpoint.url` always has a default, so
/// there is no "not configured" case to skip.
fn build_checkpoint_store(
    config: &config::ServerConfig,
) -> Result<Box<dyn laminar_core::storage::checkpoint_store::CheckpointStore>> {
    let cp = &config.checkpoint;
    let url = &cp.url;
    let max_state_data_bytes = server::resolved_checkpoint_state_bytes(cp)
        .map_err(|error| anyhow::anyhow!("checkpoint state budget: {error}"))?;

    let key_group_count = config.server.resolved_key_groups();
    let participant = if config.server.mode == config::ServerMode::Cluster {
        #[cfg(feature = "cluster")]
        {
            let node_id = config
                .node_id
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("cluster checkpoint validation requires node_id"))?;
            Some(cluster::numeric_node_id(node_id))
        }
        #[cfg(not(feature = "cluster"))]
        {
            return Err(anyhow::anyhow!(
                "cluster checkpoint validation requires a binary built with --features cluster"
            ));
        }
    } else {
        None
    };
    let participant_id = participant.unwrap_or(0);

    // Cluster runtime always uses the object-store layout, including file://, because every
    // participant has an isolated prefix on the shared base store.
    if participant.is_none() && url.starts_with("file://") {
        // Shared normalization handles the Windows drive-letter slash
        // (`file:///C:/x` must become `C:/x`, not `/C:/x`).
        let path = laminar_core::storage::object_store_builder::file_url_path(url)
            .map_err(|e| anyhow::anyhow!("checkpoint url '{url}': {e}"))?;
        let store = laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(path)
            .with_key_group_count(key_group_count)
            .with_participant_id(participant_id)
            .with_max_state_data_bytes(max_state_data_bytes)
            .map_err(|error| anyhow::anyhow!("checkpoint state budget: {error}"))?;
        Ok(Box::new(store))
    } else {
        let obj_store =
            laminar_core::storage::object_store_builder::build_object_store(url, &cp.storage)
                .map_err(|e| anyhow::anyhow!("checkpoint url '{url}': {e}"))?;
        // The builder already rooted the store at the URL's path prefix.
        let store = laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
            obj_store,
            participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
        )
        .with_key_group_count(key_group_count)
        .with_participant_id(participant_id)
        .with_max_state_data_bytes(max_state_data_bytes)
        .map_err(|error| anyhow::anyhow!("checkpoint state budget: {error}"))?;
        Ok(Box::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_checkpoint_store, checkpoint_validation_outcome, CheckpointValidationOutcome,
    };
    use laminar_core::storage::checkpoint_store::RecoveryReport;

    fn report(chosen_id: Option<u64>, examined: usize) -> RecoveryReport {
        RecoveryReport {
            chosen_id,
            skipped: Vec::new(),
            examined,
            elapsed: std::time::Duration::ZERO,
        }
    }

    #[test]
    fn validation_outcome_is_empty_only_without_checkpoint_history() {
        assert_eq!(
            checkpoint_validation_outcome(&report(None, 0)).unwrap(),
            CheckpointValidationOutcome::Empty
        );
    }

    #[test]
    fn validation_outcome_returns_structurally_valid_checkpoint() {
        assert_eq!(
            checkpoint_validation_outcome(&report(Some(42), 3)).unwrap(),
            CheckpointValidationOutcome::StructurallyValid(42)
        );
    }

    #[test]
    fn validation_outcome_rejects_nonempty_unusable_history() {
        let error = checkpoint_validation_outcome(&report(None, 2)).unwrap_err();
        assert!(error.to_string().contains("[LDB-6041]"));
    }

    #[test]
    fn checkpoint_store_uses_the_runtime_key_group_topology() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint_root = root.path().join("checkpoint-store");
        let normalized = checkpoint_root.to_string_lossy().replace('\\', "/");
        let checkpoint_url = if normalized.starts_with('/') {
            format!("file://{normalized}")
        } else {
            format!("file:///{normalized}")
        };

        let mut config: crate::config::ServerConfig = toml::from_str("").unwrap();
        config.checkpoint.url = checkpoint_url;
        let store = build_checkpoint_store(&config).unwrap();
        assert_eq!(
            store.key_group_count(),
            laminar_core::state::DEFAULT_KEY_GROUP_COUNT
        );
        assert_eq!(
            store.max_state_data_bytes(),
            laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_STATE_BYTES
        );
        assert_eq!(store.participant_id(), 0);
        assert!(
            !checkpoint_root.exists(),
            "local validation store construction must not mutate checkpoint storage"
        );
        drop(store);

        config.checkpoint.max_staged_bytes = Some(8 * 1024 * 1024);
        let store = build_checkpoint_store(&config).unwrap();
        assert_eq!(store.max_state_data_bytes(), 8 * 1024 * 1024);
        drop(store);

        #[cfg(feature = "cluster")]
        {
            let key_groups = laminar_core::state::KeyGroupCount::try_from(64_u16).unwrap();
            config.server.mode = crate::config::ServerMode::Cluster;
            config.server.key_groups = Some(key_groups);
            config.node_id = Some("checkpoint-validator".into());
            let store = build_checkpoint_store(&config).unwrap();
            assert_eq!(store.key_group_count(), key_groups);
            assert_eq!(store.max_state_data_bytes(), 8 * 1024 * 1024);
            assert_eq!(
                store.participant_id(),
                crate::cluster::numeric_node_id("checkpoint-validator")
            );
            assert!(
                checkpoint_root.exists(),
                "cluster validation must route file:// through its participant object-store layout"
            );
        }
    }

    #[test]
    fn checkpoint_store_rejects_a_zero_state_budget() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint_root = root.path().join("must-not-be-created");
        let normalized = checkpoint_root.to_string_lossy().replace('\\', "/");
        let checkpoint_url = if normalized.starts_with('/') {
            format!("file://{normalized}")
        } else {
            format!("file:///{normalized}")
        };

        let mut config: crate::config::ServerConfig = toml::from_str("").unwrap();
        config.checkpoint.url = checkpoint_url;
        config.checkpoint.max_staged_bytes = Some(0);
        let Err(error) = build_checkpoint_store(&config) else {
            panic!("zero checkpoint state budget was admitted");
        };
        assert!(
            error.to_string().contains("checkpoint state budget"),
            "{error}"
        );
        assert!(!checkpoint_root.exists());
    }

    #[test]
    fn checkpoint_store_rejects_an_unaddressable_state_budget_before_storage_setup() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint_root = root.path().join("must-not-be-created");
        let normalized = checkpoint_root.to_string_lossy().replace('\\', "/");
        let checkpoint_url = if normalized.starts_with('/') {
            format!("file://{normalized}")
        } else {
            format!("file:///{normalized}")
        };

        let mut config: crate::config::ServerConfig = toml::from_str("").unwrap();
        config.checkpoint.url = checkpoint_url;
        config.checkpoint.max_staged_bytes = Some((isize::MAX as u64) + 1);
        let Err(error) = build_checkpoint_store(&config) else {
            panic!("unaddressable checkpoint state budget was admitted");
        };
        assert!(
            error
                .to_string()
                .contains("exceeds this process address space"),
            "{error}"
        );
        assert!(!checkpoint_root.exists());
    }
}
