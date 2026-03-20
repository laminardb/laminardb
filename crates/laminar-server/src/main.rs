//! LaminarDB standalone server binary.
//!
//! Reads a TOML config file and runs streaming SQL pipelines.
//!
//! ```bash
//! laminardb --config laminardb.toml
//! ```

#![allow(clippy::disallowed_types)] // cold path: server startup and config only

mod config;
mod delta;
mod delta_config;
mod http;
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

/// LaminarDB - High-performance embedded streaming database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "laminardb.toml")]
    config: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Bind address for admin API (overrides config file)
    #[arg(long)]
    admin_bind: Option<String>,

    /// Validate checkpoints and exit without starting the server.
    ///
    /// Walks all checkpoints, verifies manifest integrity and state
    /// checksums, and reports which are valid for recovery.
    #[arg(long)]
    validate_checkpoints: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("laminardb={}", args.log_level).into()),
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

    if args.validate_checkpoints {
        return validate_checkpoints_and_exit(&config).await;
    }

    let handle = server::run_server(config, config_path).await?;
    handle.wait_for_shutdown().await?;

    Ok(())
}

/// Validate all checkpoints and exit with a report.
async fn validate_checkpoints_and_exit(config: &config::ServerConfig) -> Result<()> {
    let store = build_checkpoint_store(config);
    let Some(store) = store else {
        info!("No checkpoint configuration found — nothing to validate");
        return Ok(());
    };

    info!("Validating checkpoints...");
    let report = store
        .recover_latest_validated()
        .map_err(|e| anyhow::anyhow!("validation failed: {e}"))?;

    info!(
        "Examined {} checkpoint(s) in {:?}",
        report.examined, report.elapsed
    );
    for (id, reason) in &report.skipped {
        info!("  INVALID checkpoint {id}: {reason}");
    }
    match report.chosen_id {
        Some(id) => info!("  VALID checkpoint {id} selected for recovery"),
        None if report.examined == 0 => info!("  No checkpoints found (fresh start)"),
        None => info!("  WARNING: No valid checkpoint found — recovery would start fresh"),
    }

    // Also run orphan detection
    let orphans = store
        .cleanup_orphans()
        .map_err(|e| anyhow::anyhow!("orphan cleanup failed: {e}"))?;
    if orphans > 0 {
        info!("Cleaned up {orphans} orphaned state file(s)");
    }

    Ok(())
}

/// Build a checkpoint store from server config (shared between validate and run).
fn build_checkpoint_store(
    config: &config::ServerConfig,
) -> Option<Box<dyn laminar_storage::checkpoint_store::CheckpointStore>> {
    let cp = &config.checkpoint;
    let url = &cp.url;

    let obj_store = match laminar_storage::object_store_builder::build_object_store(
        url,
        &cp.storage,
    ) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(url = %url, error = %e, "failed to build object store for checkpoint validation");
            return None;
        }
    };

    // file:// URLs use the local FS path directly; cloud URLs need a prefix.
    if url.starts_with("file://") {
        let path = url.strip_prefix("file://").unwrap_or(url);
        Some(Box::new(
            laminar_storage::checkpoint_store::FileSystemCheckpointStore::new(
                std::path::Path::new(path),
                3,
            ),
        ))
    } else {
        // Cloud URL: extract prefix from URL path (bucket is handled by object_store).
        let prefix = url
            .split("://")
            .nth(1)
            .and_then(|rest| rest.split_once('/').map(|(_, p)| format!("{p}/")))
            .unwrap_or_default();
        match laminar_storage::checkpoint_store::ObjectStoreCheckpointStore::new(
            obj_store, prefix, 3,
        ) {
            Ok(s) => Some(Box::new(s)),
            Err(e) => {
                tracing::error!(error = %e, "failed to create checkpoint store runtime");
                None
            }
        }
    }
}
