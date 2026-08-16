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

    let handle = server::run_server(config, config_path).await?;
    handle.wait_for_shutdown().await?;

    Ok(())
}
