use std::time::Duration;

use anyhow::{anyhow, Context, Result};

pub mod config;
pub mod engine;
pub mod orders;
pub mod types;

mod live_feed;
mod server;
mod state;

use config::AppConfig;
use engine::{EngineMonitor, PipelineHarness};
use orders::OrderService;
use server::ServerState;
use state::{start_feed_watchdog, start_subscription_tasks, EventHub, FeedPhase};

/// Run the embedded engine, live market feed, and interactive dashboard until shutdown.
pub async fn run(config: AppConfig) -> Result<()> {
    config.validate()?;
    let pipeline = PipelineHarness::start(
        &config.feed_url,
        config.feed_start_timeout(),
        config.feed_stale_after(),
    )
    .await?;
    let hub = EventHub::new();
    let subscriptions = match start_subscription_tasks(&pipeline, hub.clone()).await {
        Ok(subscriptions) => subscriptions,
        Err(error) => {
            return match pipeline.shutdown().await {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("LaminarDB shutdown also failed: {cleanup:#}")))
                }
            };
        }
    };
    let watchdog = start_feed_watchdog(
        hub.clone(),
        config.feed_start_timeout(),
        config.feed_stale_after(),
    );
    let orders = OrderService::new(
        pipeline.inputs(),
        hub.clone(),
        config.feed_stale_after(),
        config.maker_fee_bps,
    );
    let monitor = pipeline.monitor();
    let server_state = ServerState::new(
        hub.clone(),
        orders,
        monitor.clone(),
        pipeline.pipeline_sql(),
    );

    let server_result = tokio::select! {
        result = server::serve(config.host, config.port, server_state) => result,
        error = wait_for_required_feed(hub, monitor) => Err(error),
    };
    watchdog.stop().await;
    subscriptions.stop().await;
    let engine_result = pipeline.shutdown().await;

    if let Err(error) = server_result {
        return match engine_result {
            Ok(()) => Err(error),
            Err(engine_error) => {
                Err(error.context(format!("LaminarDB shutdown also failed: {engine_error:#}")))
            }
        };
    }
    engine_result.context("shut down embedded LaminarDB")
}

async fn wait_for_required_feed(hub: EventHub, monitor: EngineMonitor) -> anyhow::Error {
    loop {
        let engine = monitor.snapshot();
        if engine.state != "Running" {
            let detail = engine
                .fault
                .unwrap_or_else(|| format!("LaminarDB entered state {}", engine.state));
            return anyhow!("required live market feed stopped: {detail}");
        }

        let feed = hub.status().await;
        if matches!(feed.phase, FeedPhase::Unavailable | FeedPhase::Faulted) {
            return anyhow!("required live market feed stopped: {}", feed.message);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
