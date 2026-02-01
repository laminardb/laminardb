//! Application-level backward ASOF merge for enriching orders with market ticks.
//!
//! For each order, finds the most recent tick with the same symbol where
//! `tick.ts <= order.ts`. Uses a BTreeMap index for O(log n) lookups.

use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::types::EnrichedOrder;

/// Tick data stored in the index: (price, bid, ask).
pub type TickData = (f64, f64, f64);

/// Index of recent ticks per symbol, keyed by timestamp.
pub type TickIndex = HashMap<String, BTreeMap<i64, TickData>>;

/// Merge orders with the most recent tick for each symbol (backward ASOF).
///
/// For each order tuple `(order_id, symbol, side, quantity, price, ts)`,
/// finds the latest tick in `tick_index[symbol]` where `tick.ts <= order.ts`.
pub fn merge_orders_with_ticks(
    orders: &[(String, String, String, i64, f64, i64)],
    tick_index: &TickIndex,
) -> Vec<EnrichedOrder> {
    let mut enriched = Vec::new();

    for (order_id, symbol, side, quantity, price, ts) in orders {
        if let Some(ticks) = tick_index.get(symbol.as_str()) {
            // Backward ASOF: find most recent tick where tick.ts <= order.ts
            if let Some((_, &(mkt_price, bid, ask))) = ticks.range(..=*ts).next_back() {
                enriched.push(EnrichedOrder {
                    order_id: order_id.clone(),
                    symbol: symbol.clone(),
                    side: side.clone(),
                    quantity: *quantity,
                    order_price: *price,
                    market_price: mkt_price,
                    bid,
                    ask,
                    slippage: *price - mkt_price,
                });
            }
        }
    }

    enriched
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backward_asof_merge() {
        let mut tick_index = TickIndex::new();
        let mut aapl_ticks = BTreeMap::new();
        aapl_ticks.insert(1000, (150.0, 149.95, 150.05));
        aapl_ticks.insert(2000, (151.0, 150.95, 151.05));
        aapl_ticks.insert(3000, (152.0, 151.95, 152.05));
        tick_index.insert("AAPL".to_string(), aapl_ticks);

        let orders = vec![(
            "ORD-001".to_string(),
            "AAPL".to_string(),
            "buy".to_string(),
            50,
            151.50,
            2500_i64, // between tick@2000 and tick@3000
        )];

        let result = merge_orders_with_ticks(&orders, &tick_index);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].market_price, 151.0); // tick@2000
        assert!((result[0].slippage - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_no_matching_tick() {
        let tick_index = TickIndex::new(); // empty
        let orders = vec![(
            "ORD-001".to_string(),
            "AAPL".to_string(),
            "buy".to_string(),
            50,
            151.50,
            2500_i64,
        )];

        let result = merge_orders_with_ticks(&orders, &tick_index);
        assert!(result.is_empty());
    }

    #[test]
    fn test_exact_timestamp_match() {
        let mut tick_index = TickIndex::new();
        let mut ticks = BTreeMap::new();
        ticks.insert(1000, (100.0, 99.9, 100.1));
        tick_index.insert("MSFT".to_string(), ticks);

        let orders = vec![(
            "ORD-002".to_string(),
            "MSFT".to_string(),
            "sell".to_string(),
            10,
            100.5,
            1000_i64, // exact match
        )];

        let result = merge_orders_with_ticks(&orders, &tick_index);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].market_price, 100.0);
    }

    #[test]
    fn test_multiple_symbols() {
        let mut tick_index = TickIndex::new();
        let mut aapl = BTreeMap::new();
        aapl.insert(1000, (150.0, 149.9, 150.1));
        tick_index.insert("AAPL".to_string(), aapl);

        let mut googl = BTreeMap::new();
        googl.insert(1000, (175.0, 174.9, 175.1));
        tick_index.insert("GOOGL".to_string(), googl);

        let orders = vec![
            ("ORD-1".to_string(), "AAPL".to_string(), "buy".to_string(), 10, 150.5, 1500),
            ("ORD-2".to_string(), "GOOGL".to_string(), "sell".to_string(), 20, 174.0, 1500),
        ];

        let result = merge_orders_with_ticks(&orders, &tick_index);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].symbol, "AAPL");
        assert_eq!(result[0].market_price, 150.0);
        assert_eq!(result[1].symbol, "GOOGL");
        assert_eq!(result[1].market_price, 175.0);
    }
}
