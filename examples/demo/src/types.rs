//! Typed structs for market data events and analytics results.

#![allow(dead_code)]

use laminar_derive::{FromRow, Record};

// -- Input event types (pushed into sources) --

/// A market tick event (price update).
#[derive(Debug, Clone, Record)]
pub struct MarketTick {
    pub symbol: String,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: i64,
    pub side: String,
    #[event_time]
    pub ts: i64,
}

/// An order fill event.
#[derive(Debug, Clone, Record)]
pub struct OrderEvent {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i64,
    pub price: f64,
    #[event_time]
    pub ts: i64,
}

// -- Output analytics types (read from query results) --

/// OHLC bar aggregation per symbol.
#[derive(Debug, Clone, FromRow)]
pub struct OhlcBar {
    pub symbol: String,
    pub min_price: f64,
    pub max_price: f64,
    pub trade_count: i64,
    pub total_volume: i64,
    pub vwap: f64,
}

/// Buy/sell volume breakdown per symbol.
#[derive(Debug, Clone, FromRow)]
pub struct VolumeMetrics {
    pub symbol: String,
    pub buy_volume: i64,
    pub sell_volume: i64,
    pub net_volume: i64,
    pub trade_count: i64,
}

/// Bid-ask spread statistics per symbol.
#[derive(Debug, Clone, FromRow)]
pub struct SpreadMetrics {
    pub symbol: String,
    pub avg_spread: f64,
    pub min_spread: f64,
    pub max_spread: f64,
}

/// High-volume anomaly detection per symbol.
#[derive(Debug, Clone, FromRow)]
pub struct AnomalyAlert {
    pub symbol: String,
    pub trade_count: i64,
    pub total_volume: i64,
}

// -- Kafka-mode types for JSON serialization --

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaMarketTick {
    pub symbol: String,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: i64,
    pub side: String,
    pub ts: i64,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaOrderEvent {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i64,
    pub price: f64,
    pub ts: i64,
}
