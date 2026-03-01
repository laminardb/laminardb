//! LaminarDB standalone server binary.
//!
//! Reads a TOML config file and runs streaming SQL pipelines.
//!
//! ```bash
//! laminardb --config laminardb.toml
//! ```

#![allow(clippy::disallowed_types)]

mod config;
mod http;
mod server;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing
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

    // Load configuration
    let config_path = PathBuf::from(&args.config);
    let mut config = config::load_config(&config_path)?;

    // Override bind address from CLI if provided
    if let Some(bind) = args.admin_bind {
        config.server.bind = bind;
    }

    // Build and start server
    let handle = server::run_server(config, config_path).await?;

    // Block until shutdown signal
    handle.wait_for_shutdown().await?;

    Ok(())
}
