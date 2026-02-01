//! Synthetic market data generator with stateful random-walk prices.

use rand::Rng;

use crate::types::{MarketTick, OrderEvent};

/// Symbols and their base prices.
pub const SYMBOLS: &[(&str, f64)] = &[
    ("AAPL", 150.0),
    ("GOOGL", 175.0),
    ("MSFT", 420.0),
    ("TSLA", 250.0),
    ("AMZN", 185.0),
];

const SIDES: &[&str] = &["buy", "sell"];

/// Per-symbol price state for random-walk generation.
pub struct SymbolState {
    pub symbol: &'static str,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volatility: f64,
}

impl SymbolState {
    fn new(symbol: &'static str, base_price: f64) -> Self {
        let spread = base_price * 0.0003; // 3 bps spread
        Self {
            symbol,
            price: base_price,
            bid: base_price - spread / 2.0,
            ask: base_price + spread / 2.0,
            volatility: base_price * 0.001, // 10 bps volatility
        }
    }

    fn step(&mut self, rng: &mut impl Rng) {
        // Random walk with mean reversion
        let drift: f64 = rng.gen_range(-1.0..=1.0) * self.volatility;
        self.price = (self.price + drift).max(1.0);

        // Spread widens randomly
        let spread_factor = if rng.gen_bool(0.05) { 3.0 } else { 1.0 };
        let half_spread = self.price * 0.00015 * spread_factor;
        self.bid = self.price - half_spread;
        self.ask = self.price + half_spread;
    }
}

/// Aggregate generator state for all symbols.
pub struct MarketGenerator {
    pub states: Vec<SymbolState>,
    order_seq: u64,
}

impl Default for MarketGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketGenerator {
    pub fn new() -> Self {
        let states = SYMBOLS
            .iter()
            .map(|(sym, price)| SymbolState::new(sym, *price))
            .collect();
        Self {
            states,
            order_seq: 0,
        }
    }

    /// Generate a batch of market ticks across all symbols.
    pub fn generate_ticks(&mut self, count_per_symbol: usize, base_ts: i64) -> Vec<MarketTick> {
        let mut rng = rand::thread_rng();
        let mut ticks = Vec::with_capacity(count_per_symbol * self.states.len());

        for (tick_idx, _) in (0..count_per_symbol).enumerate() {
            for state in &mut self.states {
                state.step(&mut rng);
                let volume = if rng.gen_bool(0.05) {
                    rng.gen_range(500..5000) // occasional spike
                } else {
                    rng.gen_range(10..200)
                };
                ticks.push(MarketTick {
                    symbol: state.symbol.to_string(),
                    price: round2(state.price),
                    bid: round2(state.bid),
                    ask: round2(state.ask),
                    volume,
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

        for (tick_idx, _) in (0..count_per_symbol).enumerate() {
            for state in &mut self.states {
                state.step(&mut rng);
                let volume = if rng.gen_bool(0.05) {
                    rng.gen_range(500..5000)
                } else {
                    rng.gen_range(10..200)
                };
                ticks.push(KafkaMarketTick {
                    symbol: state.symbol.to_string(),
                    price: round2(state.price),
                    bid: round2(state.bid),
                    ask: round2(state.ask),
                    volume,
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
