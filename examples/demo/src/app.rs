//! Application state for the market data demo dashboard.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use laminar_db::{PipelineMetrics, PipelineTopology};

use crate::asof_merge::TickIndex;
use crate::generator::SYMBOLS;
use crate::types::{
    AnomalyAlert, BookImbalanceMetrics, DepthMetrics, EnrichedOrder, MarketTick, OhlcBar,
    OrderBookLevel, OrderBookUpdate, SpreadMetrics, SymbolBookState, SystemStats, ViewMode,
    VolumeMetrics,
};

/// Maximum number of price history points for sparklines.
const SPARKLINE_HISTORY: usize = 60;

/// Maximum number of alerts to keep.
const MAX_ALERTS: usize = 20;

/// Maximum number of enriched orders to display.
const MAX_ENRICHED_ORDERS: usize = 8;

/// How long to keep ticks in the ASOF buffer (30 seconds in ms).
const TICK_BUFFER_WINDOW_MS: i64 = 30_000;

/// Volume threshold multiplier for anomaly detection.
const ANOMALY_VOLUME_THRESHOLD: i64 = 2000;

/// Maximum price levels per side in the order book.
const MAX_BOOK_LEVELS: usize = 10;

/// Maximum imbalance history points for sparkline.
const IMBALANCE_HISTORY_LEN: usize = 60;

/// Main application state.
pub struct App {
    // -- Pipeline state --
    pub total_ticks: u64,
    pub total_orders: u64,
    pub total_book_updates: u64,
    pub cycle: u64,
    pub start_time: Instant,
    pub paused: bool,
    pub should_quit: bool,

    // -- Per-symbol analytics --
    pub ohlc: HashMap<String, OhlcBar>,
    pub volume: HashMap<String, VolumeMetrics>,
    pub spread: HashMap<String, SpreadMetrics>,

    // -- Price history for sparklines --
    pub price_history: HashMap<String, VecDeque<f64>>,
    pub selected_symbol_idx: usize,

    // -- Alerts --
    pub alerts: VecDeque<AlertEntry>,

    // -- View mode --
    pub view_mode: ViewMode,
    pub topology: Option<PipelineTopology>,

    // -- ASOF join state --
    pub tick_buffer: TickIndex,
    pub enriched_orders: VecDeque<EnrichedOrder>,

    // -- Order book state --
    pub order_books: HashMap<String, SymbolBookState>,
    pub book_imbalance: HashMap<String, BookImbalanceMetrics>,
    pub depth_metrics: HashMap<String, DepthMetrics>,
    pub microprices: HashMap<String, f64>,
    pub imbalance_history: HashMap<String, VecDeque<f64>>,

    // -- System stats --
    pub system_stats: SystemStats,

    // -- Checkpoint state --
    pub last_checkpoint_epoch: Option<u64>,
    pub last_checkpoint_time: Option<Instant>,
    pub recovered_from_epoch: Option<u64>,

    // -- Pipeline metrics from F-OBS-001 --
    pub pipeline_metrics: Option<PipelineMetrics>,

    // -- Previous anomaly state for threshold detection --
    prev_anomaly: HashMap<String, i64>,
}

/// A timestamped alert message.
pub struct AlertEntry {
    pub time: String,
    pub message: String,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    pub fn new() -> Self {
        let mut price_history = HashMap::new();
        for (sym, _, _) in SYMBOLS {
            price_history.insert(sym.to_string(), VecDeque::with_capacity(SPARKLINE_HISTORY));
        }

        Self {
            total_ticks: 0,
            total_orders: 0,
            total_book_updates: 0,
            cycle: 0,
            start_time: Instant::now(),
            paused: false,
            should_quit: false,
            ohlc: HashMap::new(),
            volume: HashMap::new(),
            spread: HashMap::new(),
            price_history,
            selected_symbol_idx: 0,
            alerts: VecDeque::with_capacity(MAX_ALERTS),
            view_mode: ViewMode::default(),
            topology: None,
            tick_buffer: TickIndex::new(),
            enriched_orders: VecDeque::with_capacity(MAX_ENRICHED_ORDERS),
            order_books: HashMap::new(),
            book_imbalance: HashMap::new(),
            depth_metrics: HashMap::new(),
            microprices: HashMap::new(),
            imbalance_history: HashMap::new(),
            system_stats: SystemStats::default(),
            last_checkpoint_epoch: None,
            last_checkpoint_time: None,
            recovered_from_epoch: None,
            pipeline_metrics: None,
            prev_anomaly: HashMap::new(),
        }
    }

    /// Currently selected symbol name for sparkline display.
    pub fn selected_symbol(&self) -> &str {
        SYMBOLS[self.selected_symbol_idx].0
    }

    /// Cycle view mode: Dashboard → OrderBook → Dag → Dashboard.
    pub fn cycle_view(&mut self) {
        self.view_mode = match self.view_mode {
            ViewMode::Dashboard => ViewMode::OrderBook,
            ViewMode::OrderBook => ViewMode::Dag,
            ViewMode::Dag => ViewMode::Dashboard,
        };
    }

    /// Set view to a specific mode, or toggle back to Dashboard if already active.
    pub fn set_or_toggle_view(&mut self, target: ViewMode) {
        if self.view_mode == target {
            self.view_mode = ViewMode::Dashboard;
        } else {
            self.view_mode = target;
        }
    }

    /// Set the pipeline topology for the DAG view.
    pub fn set_topology(&mut self, topology: PipelineTopology) {
        self.topology = Some(topology);
    }

    /// Cycle to next symbol for sparkline.
    pub fn next_symbol(&mut self) {
        self.selected_symbol_idx = (self.selected_symbol_idx + 1) % SYMBOLS.len();
    }

    /// Uptime since start.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Throughput in events per second.
    pub fn throughput(&self) -> u64 {
        let secs = self.uptime().as_secs().max(1);
        (self.total_ticks + self.total_orders + self.total_book_updates) / secs
    }

    /// Ingest OHLC bar results from subscription.
    pub fn ingest_ohlc(&mut self, rows: Vec<OhlcBar>) {
        for row in rows {
            // Track VWAP in price history for sparklines
            if let Some(history) = self.price_history.get_mut(&row.symbol) {
                if history.len() >= SPARKLINE_HISTORY {
                    history.pop_front();
                }
                history.push_back(row.vwap);
            }
            self.ohlc.insert(row.symbol.clone(), row);
        }
    }

    /// Ingest volume metrics from subscription.
    pub fn ingest_volume(&mut self, rows: Vec<VolumeMetrics>) {
        for row in rows {
            self.volume.insert(row.symbol.clone(), row);
        }
    }

    /// Ingest spread metrics from subscription.
    pub fn ingest_spread(&mut self, rows: Vec<SpreadMetrics>) {
        for row in rows {
            self.spread.insert(row.symbol.clone(), row);
        }
    }

    /// Ingest anomaly alerts and detect threshold crossings.
    pub fn ingest_anomaly(&mut self, rows: Vec<AnomalyAlert>) {
        for row in rows {
            let prev = self.prev_anomaly.get(&row.symbol).copied().unwrap_or(0);
            let delta = row.total_volume - prev;

            if delta > ANOMALY_VOLUME_THRESHOLD {
                self.add_alert(format!(
                    "{} volume spike: {} (delta +{})",
                    row.symbol, row.total_volume, delta
                ));
            }

            self.prev_anomaly
                .insert(row.symbol.clone(), row.total_volume);
        }
    }

    /// Buffer raw ticks for ASOF join matching.
    pub fn ingest_ticks_for_asof(&mut self, ticks: &[MarketTick]) {
        for tick in ticks {
            self.tick_buffer
                .entry(tick.symbol.clone())
                .or_default()
                .insert(tick.ts, (tick.price, tick.bid, tick.ask));
        }
    }

    /// Clean up old ticks from the ASOF buffer.
    pub fn cleanup_tick_buffer(&mut self, current_ts: i64) {
        let cutoff = current_ts - TICK_BUFFER_WINDOW_MS;
        for ticks in self.tick_buffer.values_mut() {
            // Remove all entries before cutoff
            *ticks = ticks.split_off(&cutoff);
        }
    }

    /// Store enriched orders from ASOF merge.
    pub fn ingest_enriched_orders(&mut self, orders: Vec<EnrichedOrder>) {
        for order in orders {
            if self.enriched_orders.len() >= MAX_ENRICHED_ORDERS {
                self.enriched_orders.pop_back();
            }
            self.enriched_orders.push_front(order);
        }
    }

    /// Apply order book updates to maintain in-memory L2 book per symbol.
    pub fn apply_book_updates(&mut self, updates: &[OrderBookUpdate]) {
        for update in updates {
            let book = self.order_books.entry(update.symbol.clone()).or_default();
            let key = (update.price_level * 100.0) as i64;
            let side_map = if update.side == "bid" {
                &mut book.bids
            } else {
                &mut book.asks
            };

            match update.action.as_str() {
                "add" | "modify" => {
                    side_map.insert(
                        key,
                        OrderBookLevel {
                            price: update.price_level,
                            quantity: update.quantity,
                            order_count: update.order_count,
                        },
                    );
                }
                "delete" => {
                    side_map.remove(&key);
                }
                "trade" => {
                    if let Some(level) = side_map.get_mut(&key) {
                        level.quantity -= update.quantity;
                        if level.quantity <= 0 {
                            side_map.remove(&key);
                        }
                    }
                }
                _ => {}
            }

            // Cap at MAX_BOOK_LEVELS per side
            trim_book_levels(&mut book.bids, MAX_BOOK_LEVELS, true);
            trim_book_levels(&mut book.asks, MAX_BOOK_LEVELS, false);
        }

        self.total_book_updates += updates.len() as u64;
        self.recompute_microprices();
    }

    /// Recompute microprice for all symbols from BBO.
    fn recompute_microprices(&mut self) {
        for (sym, book) in &self.order_books {
            if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
                let bid_qty = bb.quantity as f64;
                let ask_qty = ba.quantity as f64;
                let total = bid_qty + ask_qty;
                if total > 0.0 {
                    let microprice = (bid_qty * ba.price + ask_qty * bb.price) / total;
                    self.microprices.insert(sym.clone(), microprice);
                }
            }
        }
    }

    /// Ingest book imbalance metrics from SQL pipeline.
    pub fn ingest_book_imbalance(&mut self, rows: Vec<BookImbalanceMetrics>) {
        for row in rows {
            // Push to imbalance history for sparkline
            let history = self
                .imbalance_history
                .entry(row.symbol.clone())
                .or_insert_with(|| VecDeque::with_capacity(IMBALANCE_HISTORY_LEN));
            if history.len() >= IMBALANCE_HISTORY_LEN {
                history.pop_front();
            }
            history.push_back(row.imbalance);

            self.book_imbalance.insert(row.symbol.clone(), row);
        }
    }

    /// Ingest depth metrics from SQL pipeline.
    pub fn ingest_depth_metrics(&mut self, rows: Vec<DepthMetrics>) {
        for row in rows {
            self.depth_metrics.insert(row.symbol.clone(), row);
        }
    }

    /// Update system stats from StatsCollector.
    pub fn update_system_stats(&mut self, mut stats: SystemStats) {
        // Preserve existing history and append new sample
        stats.cpu_history = std::mem::take(&mut self.system_stats.cpu_history);
        if stats.cpu_history.len() >= SPARKLINE_HISTORY {
            stats.cpu_history.pop_front();
        }
        stats.cpu_history.push_back(stats.cpu_usage as u64);
        self.system_stats = stats;
    }

    /// Record a successful checkpoint.
    pub fn record_checkpoint(&mut self, epoch: u64) {
        self.last_checkpoint_epoch = Some(epoch);
        self.last_checkpoint_time = Some(Instant::now());
    }

    /// Time elapsed since the last checkpoint, if any.
    pub fn time_since_checkpoint(&self) -> Option<Duration> {
        self.last_checkpoint_time.map(|t| t.elapsed())
    }

    /// Update pipeline metrics snapshot from `db.metrics()`.
    pub fn update_pipeline_metrics(&mut self, metrics: PipelineMetrics) {
        self.pipeline_metrics = Some(metrics);
    }

    /// Add a timestamped alert.
    fn add_alert(&mut self, message: String) {
        let now = chrono::Local::now().format("%H:%M:%S").to_string();
        if self.alerts.len() >= MAX_ALERTS {
            self.alerts.pop_back();
        }
        self.alerts.push_front(AlertEntry { time: now, message });
    }

    /// Total volume across all symbols.
    pub fn total_volume(&self) -> i64 {
        self.ohlc.values().map(|o| o.total_volume).sum()
    }

    /// Average VWAP across all symbols.
    pub fn avg_vwap(&self) -> f64 {
        if self.ohlc.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.ohlc.values().map(|o| o.vwap).sum();
        sum / self.ohlc.len() as f64
    }

    /// Average spread across all symbols.
    pub fn avg_spread(&self) -> f64 {
        if self.spread.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.spread.values().map(|s| s.avg_spread).sum();
        sum / self.spread.len() as f64
    }

    /// Total trade count across all symbols.
    pub fn total_trades(&self) -> i64 {
        self.ohlc.values().map(|o| o.trade_count).sum()
    }

    /// Ordered list of symbols for display.
    pub fn symbols_ordered(&self) -> Vec<&'static str> {
        SYMBOLS.iter().map(|(s, _, _)| *s).collect()
    }

    /// Get sparkline data for the selected symbol as u64 values.
    pub fn sparkline_data(&self) -> Vec<u64> {
        let sym = self.selected_symbol();
        if let Some(history) = self.price_history.get(sym) {
            if history.is_empty() {
                return vec![];
            }
            // Normalize to 0-100 range for sparkline display
            let min = history.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = history.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let range = (max - min).max(0.01);
            history
                .iter()
                .map(|&v| ((v - min) / range * 100.0) as u64)
                .collect()
        } else {
            vec![]
        }
    }

    /// Get imbalance sparkline data for the selected symbol as u64 values (0-100).
    pub fn imbalance_sparkline_data(&self) -> Vec<u64> {
        let sym = self.selected_symbol();
        if let Some(history) = self.imbalance_history.get(sym) {
            history.iter().map(|&v| (v * 100.0) as u64).collect()
        } else {
            vec![]
        }
    }
}

/// Trim a BTreeMap to at most `max` levels, keeping entries closest to BBO.
/// For bids (is_bid=true): keep the highest keys (closest to spread).
/// For asks (is_bid=false): keep the lowest keys (closest to spread).
fn trim_book_levels(
    map: &mut std::collections::BTreeMap<i64, OrderBookLevel>,
    max: usize,
    is_bid: bool,
) {
    while map.len() > max {
        if is_bid {
            // Remove lowest bid (furthest from BBO)
            if let Some(&k) = map.keys().next() {
                map.remove(&k);
            }
        } else {
            // Remove highest ask (furthest from BBO)
            if let Some(&k) = map.keys().next_back() {
                map.remove(&k);
            }
        }
    }
}
