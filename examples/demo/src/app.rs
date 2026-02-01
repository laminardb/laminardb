//! Application state for the market data demo dashboard.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use laminar_db::PipelineTopology;

use crate::generator::SYMBOLS;
use crate::types::{AnomalyAlert, OhlcBar, SpreadMetrics, VolumeMetrics};

/// Maximum number of price history points for sparklines.
const SPARKLINE_HISTORY: usize = 60;

/// Maximum number of alerts to keep.
const MAX_ALERTS: usize = 20;

/// Volume threshold multiplier for anomaly detection.
const ANOMALY_VOLUME_THRESHOLD: i64 = 2000;

/// Main application state.
pub struct App {
    // -- Pipeline state --
    pub total_ticks: u64,
    pub total_orders: u64,
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

    // -- DAG view --
    pub show_dag: bool,
    pub topology: Option<PipelineTopology>,

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
        for (sym, _) in SYMBOLS {
            price_history.insert(sym.to_string(), VecDeque::with_capacity(SPARKLINE_HISTORY));
        }

        Self {
            total_ticks: 0,
            total_orders: 0,
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
            show_dag: false,
            topology: None,
            prev_anomaly: HashMap::new(),
        }
    }

    /// Currently selected symbol name for sparkline display.
    pub fn selected_symbol(&self) -> &str {
        SYMBOLS[self.selected_symbol_idx].0
    }

    /// Toggle between dashboard and DAG view.
    pub fn toggle_dag(&mut self) {
        self.show_dag = !self.show_dag;
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
        (self.total_ticks + self.total_orders) / secs
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

            self.prev_anomaly.insert(row.symbol.clone(), row.total_volume);
        }
    }

    /// Add a timestamped alert.
    fn add_alert(&mut self, message: String) {
        let now = chrono::Local::now().format("%H:%M:%S").to_string();
        if self.alerts.len() >= MAX_ALERTS {
            self.alerts.pop_back();
        }
        self.alerts.push_front(AlertEntry {
            time: now,
            message,
        });
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
        SYMBOLS.iter().map(|(s, _)| *s).collect()
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
}
