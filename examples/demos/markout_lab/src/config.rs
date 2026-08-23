use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use url::Url;

/// Default credential-free real-time market-data endpoint.
pub const DEFAULT_FEED_URL: &str = "wss://data-stream.binance.vision/ws/btcusdt@ticker";

/// Command-line options for the live Markout Lab server.
#[derive(Debug, Clone, Parser)]
#[command(name = "markout-lab", version, about)]
pub struct AppConfig {
    /// HTTP listen address.
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub host: IpAddr,

    /// HTTP listen port.
    #[arg(long, default_value_t = 8088)]
    pub port: u16,

    /// WebSocket endpoint supplying Binance-compatible ticker messages.
    #[arg(long, default_value = DEFAULT_FEED_URL)]
    pub feed_url: String,

    /// Seconds allowed for the first live quote before the feed is unavailable.
    #[arg(long, default_value_t = 15)]
    pub feed_start_timeout_secs: u64,

    /// Seconds without a quote before the demo terminates.
    #[arg(long, default_value_t = 5)]
    pub feed_stale_after_secs: u64,

    /// Hypothetical maker fee in basis points.
    #[arg(long, default_value_t = 0.0)]
    pub maker_fee_bps: f64,
}

impl AppConfig {
    /// Validate the explicit feed, freshness, and hypothetical-fee inputs.
    ///
    /// # Errors
    /// Returns an error for an invalid WebSocket URL or an unsafe bound.
    pub fn validate(&self) -> Result<()> {
        let feed_url = Url::parse(&self.feed_url).context("--feed-url is not a valid URL")?;
        if !matches!(feed_url.scheme(), "ws" | "wss") || feed_url.host().is_none() {
            bail!("--feed-url must be an absolute ws:// or wss:// URL with a host");
        }
        if self
            .feed_url
            .chars()
            .any(|character| matches!(character, '\'' | '\n' | '\r'))
        {
            bail!("--feed-url contains characters that cannot be embedded safely in SQL");
        }
        if !(1..=120).contains(&self.feed_start_timeout_secs) {
            bail!("--feed-start-timeout-secs must be between 1 and 120");
        }
        if !(2..=60).contains(&self.feed_stale_after_secs) {
            bail!("--feed-stale-after-secs must be between 2 and 60");
        }
        if !self.maker_fee_bps.is_finite() || !(-10.0..=100.0).contains(&self.maker_fee_bps) {
            bail!("--maker-fee-bps must be a finite value between -10 and 100");
        }
        Ok(())
    }

    #[must_use]
    pub const fn feed_start_timeout(&self) -> Duration {
        Duration::from_secs(self.feed_start_timeout_secs)
    }

    #[must_use]
    pub const fn feed_stale_after(&self) -> Duration {
        Duration::from_secs(self.feed_stale_after_secs)
    }
}
