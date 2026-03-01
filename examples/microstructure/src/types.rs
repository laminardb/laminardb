use laminar_derive::FromRow;

pub const SYMBOLS: &[&str] = &[
    "btcusdt", "ethusdt", "solusdt", "dogeusdt", "xrpusdt", "bnbusdt",
];
pub const LABELS: &[&str] = &["BTC", "ETH", "SOL", "DOGE", "XRP", "BNB"];

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct Spread {
    pub symbol: String,
    pub spread: f64,
    pub spread_bps: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub bid_pressure: f64,
}

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct Vwap {
    pub symbol: String,
    pub vwap: f64,
    pub avg_price: f64,
    pub low: f64,
    pub high: f64,
    pub volume: f64,
    pub trades: i64,
    pub volatility_bps: f64,
    pub maker_ratio: f64,
}

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct Momentum {
    pub symbol: String,
    pub avg_30s: f64,
    pub low_30s: f64,
    pub high_30s: f64,
    pub vol_30s: f64,
    pub trades_30s: i64,
    pub range_bps_30s: f64,
}

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct Burst {
    pub symbol: String,
    pub burst_trades: i64,
    pub burst_volume: f64,
    pub burst_range_bps: f64,
    pub maker_ratio: f64,
}

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct Signal {
    pub symbol: String,
    pub vwap: f64,
    pub avg_price: f64,
    pub volume: f64,
    pub trades: i64,
    pub volatility_bps: f64,
    pub maker_ratio: f64,
    pub signal: String,
}

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct MicroCandle {
    pub symbol: String,
    pub ticks: i64,
    pub vwap: f64,
    pub low: f64,
    pub high: f64,
    pub volume: f64,
    pub buy_vol: f64,
    pub sell_vol: f64,
}

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
pub struct DepthLevel {
    pub price: f64,
    pub qty: f64,
}
