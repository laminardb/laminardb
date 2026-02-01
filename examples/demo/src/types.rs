//! Typed structs for market data events and analytics results.

#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};

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

/// Enriched order with ASOF-joined market data.
#[derive(Debug, Clone)]
pub struct EnrichedOrder {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i64,
    pub order_price: f64,
    pub market_price: f64,
    pub bid: f64,
    pub ask: f64,
    pub slippage: f64,
}

// -- Order book types --

/// An L2 order book update event (pushed into source).
#[derive(Debug, Clone, Record)]
pub struct OrderBookUpdate {
    pub symbol: String,
    pub side: String,      // "bid" or "ask"
    pub action: String,    // "add", "modify", "delete", "trade"
    pub price_level: f64,
    pub quantity: i64,
    pub order_count: i64,
    #[event_time]
    pub ts: i64,
}

/// Book imbalance analytics from SQL pipeline.
#[derive(Debug, Clone, FromRow)]
pub struct BookImbalanceMetrics {
    pub symbol: String,
    pub bid_qty_total: i64,
    pub ask_qty_total: i64,
    pub imbalance: f64, // bid/(bid+ask), 0.0–1.0
}

/// Depth metrics from SQL pipeline.
#[derive(Debug, Clone, FromRow)]
pub struct DepthMetrics {
    pub symbol: String,
    pub total_bid_qty: i64,
    pub total_ask_qty: i64,
    pub update_count: i64,
}

/// A single price level in the order book.
#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: i64,
    pub order_count: i64,
}

/// Per-symbol L2 book state. Keys are price_cents (price * 100 as i64).
#[derive(Debug, Clone, Default)]
pub struct SymbolBookState {
    pub bids: BTreeMap<i64, OrderBookLevel>, // highest key = best bid
    pub asks: BTreeMap<i64, OrderBookLevel>, // lowest key = best ask
}

impl SymbolBookState {
    /// Best bid (highest price).
    pub fn best_bid(&self) -> Option<&OrderBookLevel> {
        self.bids.values().next_back()
    }

    /// Best ask (lowest price).
    pub fn best_ask(&self) -> Option<&OrderBookLevel> {
        self.asks.values().next()
    }

    /// Top N bid levels (best first = highest price first).
    pub fn top_bids(&self, n: usize) -> Vec<&OrderBookLevel> {
        self.bids.values().rev().take(n).collect()
    }

    /// Top N ask levels (best first = lowest price first).
    pub fn top_asks(&self, n: usize) -> Vec<&OrderBookLevel> {
        self.asks.values().take(n).collect()
    }

    /// Total bid depth (sum of all bid quantities).
    pub fn bid_depth(&self) -> i64 {
        self.bids.values().map(|l| l.quantity).sum()
    }

    /// Total ask depth (sum of all ask quantities).
    pub fn ask_depth(&self) -> i64 {
        self.asks.values().map(|l| l.quantity).sum()
    }

    /// Book imbalance: bid_depth / (bid_depth + ask_depth), 0.0–1.0.
    pub fn imbalance(&self) -> f64 {
        let bd = self.bid_depth() as f64;
        let ad = self.ask_depth() as f64;
        let total = bd + ad;
        if total == 0.0 {
            0.5
        } else {
            bd / total
        }
    }
}

/// View mode for TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ViewMode {
    #[default]
    Dashboard,
    OrderBook,
    Dag,
}

/// System resource stats (from sysinfo).
#[derive(Debug, Clone, Default)]
pub struct SystemStats {
    pub cpu_usage: f32,
    pub memory_mb: f64,
    pub total_memory_mb: f64,
    pub cpu_history: VecDeque<u64>, // last 60 samples, 0-100 for sparkline
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
impl KafkaMarketTick {
    /// Convert to app-level MarketTick for local state tracking.
    pub fn to_market_tick(&self) -> MarketTick {
        MarketTick {
            symbol: self.symbol.clone(),
            price: self.price,
            bid: self.bid,
            ask: self.ask,
            volume: self.volume,
            side: self.side.clone(),
            ts: self.ts,
        }
    }
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

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaOrderBookUpdate {
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub price_level: f64,
    pub quantity: i64,
    pub order_count: i64,
    pub ts: i64,
}

#[cfg(feature = "kafka")]
impl KafkaOrderBookUpdate {
    /// Convert to app-level OrderBookUpdate for local state tracking.
    pub fn to_order_book_update(&self) -> OrderBookUpdate {
        OrderBookUpdate {
            symbol: self.symbol.clone(),
            side: self.side.clone(),
            action: self.action.clone(),
            price_level: self.price_level,
            quantity: self.quantity,
            order_count: self.order_count,
            ts: self.ts,
        }
    }
}
