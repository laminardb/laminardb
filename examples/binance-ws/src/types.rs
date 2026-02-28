use laminar_derive::FromRow;

pub const SYMBOLS: &[&str] = &[
    "btcusdt", "ethusdt", "solusdt", "dogeusdt", "xrpusdt", "bnbusdt",
];
pub const LABELS: &[&str] = &["BTC", "ETH", "SOL", "DOGE", "XRP", "BNB"];

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
    pub signal: String,
}
