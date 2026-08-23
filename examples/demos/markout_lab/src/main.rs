use anyhow::Result;
use clap::Parser;
use markout_lab::config::AppConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("info,datafusion=warn,sqlparser=warn,arrow=warn")
            }),
        )
        .with_target(false)
        .init();
    markout_lab::run(AppConfig::parse()).await
}
