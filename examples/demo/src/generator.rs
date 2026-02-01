//! Synthetic market data generator with correlated random-walk prices,
//! mean-reverting spreads, intraday volume patterns, and L2 order book updates.

use rand::Rng;

use crate::types::{MarketTick, OrderBookUpdate, OrderEvent};

/// Symbols with base prices and beta (market sensitivity).
pub const SYMBOLS: &[(&str, f64, f64)] = &[
    //  symbol, base_price, beta
    ("AAPL", 150.0, 1.1),
    ("GOOGL", 175.0, 0.9),
    ("MSFT", 420.0, 0.8),
    ("TSLA", 250.0, 1.5),
    ("AMZN", 185.0, 1.0),
];

const SIDES: &[&str] = &["buy", "sell"];

/// Per-symbol price state for correlated random-walk generation.
pub struct SymbolState {
    pub symbol: &'static str,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volatility: f64,
    pub beta: f64,
    pub spread_mean: f64,
    pub spread_current: f64,
}

impl SymbolState {
    fn new(symbol: &'static str, base_price: f64, beta: f64) -> Self {
        let spread = base_price * 0.0003; // 3 bps spread
        Self {
            symbol,
            price: base_price,
            bid: base_price - spread / 2.0,
            ask: base_price + spread / 2.0,
            volatility: base_price * 0.001, // 10 bps volatility
            beta,
            spread_mean: spread,
            spread_current: spread,
        }
    }

    fn step(&mut self, rng: &mut impl Rng, market_factor: f64) {
        // Correlated price drift: beta * market_factor + idiosyncratic noise
        let systematic = self.beta * market_factor * self.volatility;
        let idiosyncratic: f64 = rng.gen_range(-1.0..=1.0) * self.volatility * 0.5;
        self.price = (self.price + systematic + idiosyncratic).max(1.0);

        // Ornstein-Uhlenbeck mean-reverting spread
        let spread_noise: f64 = rng.gen_range(-0.3..=0.3) * self.spread_mean;
        self.spread_current += 0.3 * (self.spread_mean - self.spread_current)
            + spread_noise;
        self.spread_current = self.spread_current.max(self.price * 0.0001); // min 1 bps

        // Spread widens occasionally
        let spread_factor = if rng.gen_bool(0.05) { 3.0 } else { 1.0 };
        let half_spread = self.spread_current * spread_factor / 2.0;
        self.bid = self.price - half_spread;
        self.ask = self.price + half_spread;
    }
}

/// Aggregate generator state for all symbols.
pub struct MarketGenerator {
    pub states: Vec<SymbolState>,
    order_seq: u64,
    pub cycle: u64,
}

impl Default for MarketGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// U-shaped intraday volume multiplier. Peaks at open/close, trough at midday.
fn intraday_volume_multiplier(cycle: u64) -> f64 {
    let t = (cycle % 300) as f64 / 300.0; // normalize to 0..1
    2.0 * (t - 0.5) * (t - 0.5) + 0.5
}

impl MarketGenerator {
    pub fn new() -> Self {
        let states = SYMBOLS
            .iter()
            .map(|(sym, price, beta)| SymbolState::new(sym, *price, *beta))
            .collect();
        Self {
            states,
            order_seq: 0,
            cycle: 0,
        }
    }

    /// Generate a batch of market ticks across all symbols.
    pub fn generate_ticks(
        &mut self,
        count_per_symbol: usize,
        base_ts: i64,
    ) -> Vec<MarketTick> {
        let mut rng = rand::thread_rng();
        let mut ticks = Vec::with_capacity(count_per_symbol * self.states.len());
        self.cycle += 1;
        let vol_mult = intraday_volume_multiplier(self.cycle);

        for tick_idx in 0..count_per_symbol {
            let market_factor: f64 = rng.gen_range(-1.0..=1.0);
            for state in &mut self.states {
                state.step(&mut rng, market_factor);
                let base_volume = if rng.gen_bool(0.05) {
                    rng.gen_range(500..5000) // occasional spike
                } else {
                    rng.gen_range(10..200)
                };
                let volume = (base_volume as f64 * vol_mult) as i64;
                ticks.push(MarketTick {
                    symbol: state.symbol.to_string(),
                    price: round2(state.price),
                    bid: round2(state.bid),
                    ask: round2(state.ask),
                    volume: volume.max(1),
                    side: SIDES[rng.gen_range(0..2)].to_string(),
                    ts: base_ts + tick_idx as i64 * 50, // 50ms apart
                });
            }
        }
        ticks
    }

    /// Generate a batch of order events across all symbols.
    pub fn generate_orders(&mut self, count: usize, base_ts: i64) -> Vec<OrderEvent> {
        let mut rng = rand::thread_rng();
        (0..count)
            .map(|i| {
                let state = &self.states[rng.gen_range(0..self.states.len())];
                self.order_seq += 1;
                OrderEvent {
                    order_id: format!("ORD-{:08}", self.order_seq),
                    symbol: state.symbol.to_string(),
                    side: SIDES[rng.gen_range(0..2)].to_string(),
                    quantity: rng.gen_range(1..=100),
                    price: round2(state.price),
                    ts: base_ts + i as i64 * 100,
                }
            })
            .collect()
    }

    /// Generate L2 order book updates for all symbols.
    /// Produces 3–8 updates per symbol per call.
    pub fn generate_book_updates(&self, base_ts: i64) -> Vec<OrderBookUpdate> {
        let mut rng = rand::thread_rng();
        let mut updates = Vec::new();

        for (sym_idx, state) in self.states.iter().enumerate() {
            let n_updates = rng.gen_range(3..=8);
            let tick_size = (state.price * 0.0001).max(0.01); // min $0.01

            for j in 0..n_updates {
                let side = if rng.gen_bool(0.5) { "bid" } else { "ask" };
                let level_offset = rng.gen_range(0..10) as f64;
                let price_level = if side == "bid" {
                    round2(state.bid - level_offset * tick_size)
                } else {
                    round2(state.ask + level_offset * tick_size)
                };

                // Action distribution: 30% add, 50% modify, 15% delete, 5% trade
                let r: f64 = rng.gen();
                let (action, quantity, order_count) = if r < 0.30 {
                    ("add", rng.gen_range(10..=500), rng.gen_range(1..=10))
                } else if r < 0.80 {
                    ("modify", rng.gen_range(10..=500), rng.gen_range(1..=10))
                } else if r < 0.95 {
                    ("delete", 0_i64, 0_i64)
                } else {
                    ("trade", rng.gen_range(1..=100), 1_i64)
                };

                let ts = base_ts + (sym_idx * 10 + j) as i64 * 25; // 25ms spacing

                updates.push(OrderBookUpdate {
                    symbol: state.symbol.to_string(),
                    side: side.to_string(),
                    action: action.to_string(),
                    price_level,
                    quantity,
                    order_count,
                    ts,
                });
            }
        }
        updates
    }
}

// -- Kafka mode generators --

#[cfg(feature = "kafka")]
impl MarketGenerator {
    pub fn generate_kafka_ticks(
        &mut self,
        count_per_symbol: usize,
        base_ts: i64,
    ) -> Vec<crate::types::KafkaMarketTick> {
        use crate::types::KafkaMarketTick;
        let mut rng = rand::thread_rng();
        let mut ticks = Vec::with_capacity(count_per_symbol * self.states.len());
        self.cycle += 1;
        let vol_mult = intraday_volume_multiplier(self.cycle);

        for tick_idx in 0..count_per_symbol {
            let market_factor: f64 = rng.gen_range(-1.0..=1.0);
            for state in &mut self.states {
                state.step(&mut rng, market_factor);
                let base_volume = if rng.gen_bool(0.05) {
                    rng.gen_range(500..5000)
                } else {
                    rng.gen_range(10..200)
                };
                let volume = (base_volume as f64 * vol_mult) as i64;
                ticks.push(KafkaMarketTick {
                    symbol: state.symbol.to_string(),
                    price: round2(state.price),
                    bid: round2(state.bid),
                    ask: round2(state.ask),
                    volume: volume.max(1),
                    side: SIDES[rng.gen_range(0..2)].to_string(),
                    ts: base_ts + tick_idx as i64 * 50,
                });
            }
        }
        ticks
    }

    pub fn generate_kafka_orders(
        &mut self,
        count: usize,
        base_ts: i64,
    ) -> Vec<crate::types::KafkaOrderEvent> {
        use crate::types::KafkaOrderEvent;
        let mut rng = rand::thread_rng();
        (0..count)
            .map(|i| {
                let state = &self.states[rng.gen_range(0..self.states.len())];
                self.order_seq += 1;
                KafkaOrderEvent {
                    order_id: format!("ORD-{:08}", self.order_seq),
                    symbol: state.symbol.to_string(),
                    side: SIDES[rng.gen_range(0..2)].to_string(),
                    quantity: rng.gen_range(1..=100),
                    price: round2(state.price),
                    ts: base_ts + i as i64 * 100,
                }
            })
            .collect()
    }

    pub fn generate_kafka_book_updates(
        &self,
        base_ts: i64,
    ) -> Vec<crate::types::KafkaOrderBookUpdate> {
        use crate::types::KafkaOrderBookUpdate;
        let mut rng = rand::thread_rng();
        let mut updates = Vec::new();

        for (sym_idx, state) in self.states.iter().enumerate() {
            let n_updates = rng.gen_range(3..=8);
            let tick_size = (state.price * 0.0001).max(0.01);

            for j in 0..n_updates {
                let side = if rng.gen_bool(0.5) { "bid" } else { "ask" };
                let level_offset = rng.gen_range(0..10) as f64;
                let price_level = if side == "bid" {
                    round2(state.bid - level_offset * tick_size)
                } else {
                    round2(state.ask + level_offset * tick_size)
                };

                let r: f64 = rng.gen();
                let (action, quantity, order_count) = if r < 0.30 {
                    ("add", rng.gen_range(10..=500), rng.gen_range(1..=10))
                } else if r < 0.80 {
                    ("modify", rng.gen_range(10..=500), rng.gen_range(1..=10))
                } else if r < 0.95 {
                    ("delete", 0_i64, 0_i64)
                } else {
                    ("trade", rng.gen_range(1..=100), 1_i64)
                };

                let ts = base_ts + (sym_idx * 10 + j) as i64 * 25;

                updates.push(KafkaOrderBookUpdate {
                    symbol: state.symbol.to_string(),
                    side: side.to_string(),
                    action: action.to_string(),
                    price_level,
                    quantity,
                    order_count,
                    ts,
                });
            }
        }
        updates
    }
}

#[cfg(feature = "kafka")]
pub async fn produce_to_kafka<T: serde::Serialize>(
    producer: &rdkafka::producer::FutureProducer,
    topic: &str,
    events: &[T],
) -> Result<usize, Box<dyn std::error::Error>> {
    use rdkafka::producer::FutureRecord;

    let mut count = 0;
    for event in events {
        let json = serde_json::to_string(event)?;
        let record = FutureRecord::<(), _>::to(topic).payload(&json);
        producer
            .send(record, std::time::Duration::from_secs(5))
            .await
            .map_err(|(e, _)| e)?;
        count += 1;
    }
    Ok(count)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}
