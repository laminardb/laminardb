use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BinaryBuilder, UInt32Array};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceRowPositionCapability, SourceRowPositions, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::registry::ConnectorRegistry;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::types::{live_quote_schema, live_quote_to_batch, LiveQuote, SYMBOL};

pub const CONNECTOR_NAME: &str = "markout-live-binance";
const MAX_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug, Clone)]
struct LiveFeedConfig {
    url: String,
    connect_timeout: Duration,
    read_timeout: Duration,
}

impl LiveFeedConfig {
    fn from_connector(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config.require("url")?.to_string();
        let parsed = Url::parse(&url).map_err(|error| {
            ConnectorError::ConfigurationError(format!("invalid live feed URL: {error}"))
        })?;
        if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host().is_none() {
            return Err(ConnectorError::ConfigurationError(
                "live feed URL must be an absolute ws:// or wss:// URL".to_string(),
            ));
        }
        for key in config.properties().keys() {
            if !matches!(
                key.as_str(),
                "url"
                    | "connect.timeout.ms"
                    | "read.timeout.ms"
                    | "laminar.source.name"
                    | "_arrow_schema"
            ) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown live feed option '{key}'"
                )));
            }
        }
        let connect_timeout_ms = timeout_ms(config, "connect.timeout.ms", 15_000)?;
        let read_timeout_ms = timeout_ms(config, "read.timeout.ms", 5_000)?;
        Ok(Self {
            url,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: Duration::from_millis(read_timeout_ms),
        })
    }
}

fn timeout_ms(config: &ConnectorConfig, key: &str, default: u64) -> Result<u64, ConnectorError> {
    let timeout = config.get_parsed(key)?.unwrap_or(default);
    if !(1..=MAX_TIMEOUT_MS).contains(&timeout) {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} must be between 1 and {MAX_TIMEOUT_MS}"
        )));
    }
    Ok(timeout)
}

#[derive(Debug, Deserialize)]
struct BinanceTicker {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_ts_ms: i64,
    #[serde(rename = "C")]
    sequence: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid_px: String,
    #[serde(rename = "a")]
    ask_px: String,
    #[serde(rename = "B")]
    bid_qty: String,
    #[serde(rename = "A")]
    ask_qty: String,
}

impl BinanceTicker {
    fn into_quote(self) -> Result<LiveQuote, ConnectorError> {
        if self.event_type != "24hrTicker" {
            return Err(fatal_feed(format!(
                "unexpected Binance event type '{}'",
                self.event_type
            )));
        }
        let event_ts = self
            .event_ts_ms
            .checked_mul(1_000)
            .ok_or_else(|| fatal_feed("event timestamp overflow"))?;
        let quote = LiveQuote {
            event_type: self.event_type,
            sequence: self.sequence,
            symbol: self.symbol,
            bid_px: parse_number("best bid", &self.bid_px)?,
            ask_px: parse_number("best ask", &self.ask_px)?,
            bid_qty: parse_number("best bid quantity", &self.bid_qty)?,
            ask_qty: parse_number("best ask quantity", &self.ask_qty)?,
            event_ts,
        };
        if quote.sequence < 0
            || quote.event_ts < 0
            || quote.symbol != SYMBOL
            || quote.bid_px <= 0.0
            || quote.ask_px < quote.bid_px
            || quote.bid_qty < 0.0
            || quote.ask_qty < 0.0
        {
            return Err(fatal_feed(
                "Binance ticker contained invalid top-of-book values",
            ));
        }
        Ok(quote)
    }
}

fn parse_number(field: &str, value: &str) -> Result<f64, ConnectorError> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| fatal_feed(format!("invalid Binance {field} '{value}': {error}")))?;
    if !parsed.is_finite() {
        return Err(fatal_feed(format!("Binance {field} is not finite")));
    }
    Ok(parsed)
}

fn fatal_feed(actual: impl Into<String>) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "an active, ordered Binance BTCUSDT ticker stream".to_string(),
        actual: actual.into(),
    }
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct LiveTickerSource {
    schema: SchemaRef,
    config: Option<LiveFeedConfig>,
    socket: Option<Socket>,
    last_position: Option<(i64, i64)>,
}

impl LiveTickerSource {
    fn new() -> Self {
        Self {
            schema: live_quote_schema(),
            config: None,
            socket: None,
            last_position: None,
        }
    }

    fn positions(&self, quote: &LiveQuote) -> Result<SourceRowPositions, ConnectorError> {
        let mut partitions = BinaryBuilder::with_capacity(1, quote.symbol.len());
        partitions.append_value(quote.symbol.as_bytes());
        let mut order_keys = BinaryBuilder::with_capacity(1, 16);
        let mut order_key = [0_u8; 16];
        order_key[..8].copy_from_slice(&quote.sequence.to_be_bytes());
        order_key[8..].copy_from_slice(&quote.event_ts.to_be_bytes());
        order_keys.append_value(order_key);
        SourceRowPositions::try_new(
            partitions.finish(),
            order_keys.finish(),
            UInt32Array::from(vec![0]),
        )
    }

    fn validate_position(&mut self, quote: &LiveQuote) -> Result<(), ConnectorError> {
        let position = (quote.sequence, quote.event_ts);
        if self
            .last_position
            .is_some_and(|previous| position <= previous)
        {
            return Err(fatal_feed(format!(
                "Binance ticker position {position:?} did not advance beyond {:?}",
                self.last_position
            )));
        }
        self.last_position = Some(position);
        Ok(())
    }

    async fn next_quote(&mut self) -> Result<LiveQuote, ConnectorError> {
        loop {
            let read_timeout = self
                .config
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "configured live Binance source".to_string(),
                    actual: "source was polled before start".to_string(),
                })?
                .read_timeout;
            let socket = self
                .socket
                .as_mut()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "connected live Binance source".to_string(),
                    actual: "source was polled before start".to_string(),
                })?;
            let message = tokio::time::timeout(read_timeout, socket.next())
                .await
                .map_err(|_| fatal_feed(format!("no message for {read_timeout:?}")))?
                .ok_or_else(|| fatal_feed("Binance WebSocket stream ended"))?
                .map_err(|error| fatal_feed(format!("WebSocket read failed: {error}")))?;
            match message {
                Message::Text(text) => {
                    let ticker: BinanceTicker = serde_json::from_str(text.as_ref())
                        .map_err(|error| fatal_feed(format!("invalid ticker JSON: {error}")))?;
                    return ticker.into_quote();
                }
                Message::Binary(data) => {
                    let ticker: BinanceTicker = serde_json::from_slice(&data)
                        .map_err(|error| fatal_feed(format!("invalid ticker JSON: {error}")))?;
                    return ticker.into_quote();
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|error| fatal_feed(format!("WebSocket pong failed: {error}")))?;
                }
                Message::Close(frame) => {
                    return Err(fatal_feed(format!(
                        "Binance closed the live feed: {frame:?}"
                    )));
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

#[async_trait]
impl SourceConnector for LiveTickerSource {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _delivery) = request.into_parts();
        if !matches!(position, SourcePosition::Initial) {
            return Err(ConnectorError::ConfigurationError(
                "the live Binance source cannot resume or replay".to_string(),
            ));
        }
        let config = LiveFeedConfig::from_connector(&config)?;
        let connected = tokio::time::timeout(
            config.connect_timeout,
            tokio_tungstenite::connect_async(&config.url),
        )
        .await
        .map_err(|_| {
            ConnectorError::Timeout(
                u64::try_from(config.connect_timeout.as_millis()).unwrap_or(u64::MAX),
            )
        })?
        .map_err(|error| ConnectorError::ConnectionFailed(error.to_string()))?;
        self.socket = Some(connected.0);
        self.config = Some(config);
        self.last_position = None;
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if max_records == 0 {
            return Ok(None);
        }
        let quote = self.next_quote().await?;
        self.validate_position(&quote)?;
        let positions = self.positions(&quote)?;
        let batch = live_quote_to_batch(&quote)
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
        Ok(Some(SourceBatch::positioned(batch, positions)?))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_metadata("connector", CONNECTOR_NAME);
        if let Some((sequence, event_ts)) = self.last_position {
            checkpoint.set_offset("sequence", sequence.to_string());
            checkpoint.set_offset("event_ts", event_ts.to_string());
        }
        checkpoint
            .set_input_channels(vec![SYMBOL.as_bytes().to_vec()])
            .expect("the static BTCUSDT input channel is valid");
        checkpoint
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        LiveFeedConfig::from_connector(config)?;
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut socket) = self.socket.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), socket.send(Message::Close(None)))
                .await;
        }
        self.last_position = None;
        Ok(())
    }
}

pub fn register_live_feed(registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
    registry.register_source(
        CONNECTOR_NAME,
        ConnectorInfo {
            name: CONNECTOR_NAME.to_string(),
            display_name: "Markout Lab live Binance ticker".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            is_source: true,
            is_sink: false,
            config_keys: Vec::new(),
        },
        Arc::new(|_| Ok(Box::new(LiveTickerSource::new()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_decoding_requires_valid_ordered_top_of_book() {
        let ticker: BinanceTicker = serde_json::from_str(
            r#"{"e":"24hrTicker","E":1735689600000,"C":1735689600001,"s":"BTCUSDT","b":"94000.00","B":"1.2","a":"94000.10","A":"2.3"}"#,
        )
        .unwrap();
        let quote = ticker.into_quote().unwrap();
        assert_eq!(quote.symbol, "BTCUSDT");
        assert_eq!(quote.event_ts, 1_735_689_600_000_000);
        assert_eq!(quote.bid_px, 94_000.0);
        assert_eq!(quote.ask_px, 94_000.1);
    }

    #[test]
    fn ticker_decoding_rejects_crossed_prices() {
        let ticker = BinanceTicker {
            event_type: "24hrTicker".to_string(),
            event_ts_ms: 1,
            sequence: 1,
            symbol: "BTCUSDT".to_string(),
            bid_px: "2".to_string(),
            ask_px: "1".to_string(),
            bid_qty: "1".to_string(),
            ask_qty: "1".to_string(),
        };
        assert!(ticker.into_quote().is_err());
    }
}
