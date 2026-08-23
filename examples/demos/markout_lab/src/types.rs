//! Arrow input contracts and typed LaminarDB subscription rows.

use std::sync::Arc;

use anyhow::Result;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use laminar_derive::FromRow;
use serde::Serialize;

/// Binance Spot symbol used by the live source and temporal join.
pub const SYMBOL: &str = "BTCUSDT";
/// Truthful fill-model label carried through every fill and markout.
pub const FILL_MODEL: &str = "simulated_live_touch";
/// Canonical temporal-probe horizons in milliseconds.
pub const HORIZONS_MS: [i64; 5] = [0, 1_000, 5_000, 15_000, 30_000];

/// One validated Binance ticker update produced by the live connector.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveQuote {
    pub event_type: String,
    pub sequence: i64,
    pub symbol: String,
    pub bid_px: f64,
    pub ask_px: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    /// Exchange event timestamp in microseconds since the Unix epoch.
    pub event_ts: i64,
}

/// One visitor-requested, touch-based simulated maker fill sent into LaminarDB.
#[derive(Debug, Clone, PartialEq)]
pub struct SimulatedFill {
    /// Browser-player identity used to isolate aggregate results.
    pub demo_run_id: String,
    /// Stable fill identity within the run.
    pub fill_id: String,
    /// Quote sequence that caused this opportunity.
    pub source_event_id: i64,
    /// Interactive strategy identifier.
    pub strategy: String,
    /// Instrument identifier.
    pub symbol: String,
    /// Simulated maker side: `BUY` or `SELL`.
    pub side: String,
    /// Simulated base-asset quantity.
    pub quantity: f64,
    /// Touch price used by the fill model.
    pub fill_px: f64,
    /// Configured hypothetical fee in basis points.
    pub fee_bps: f64,
    /// Midpoint observed at execution time.
    pub mid_at_fill: f64,
    /// Spread observed at execution time in basis points.
    pub spread_bps_at_fill: f64,
    /// Human-readable visitor action explanation.
    pub decision_reason: String,
    /// Fill-model truthfulness label.
    pub fill_model: String,
    /// Event timestamp in microseconds since the Unix epoch.
    pub event_ts: i64,
}

/// Engine-produced quote presentation row.
#[derive(Debug, Clone, PartialEq, FromRow, Serialize)]
pub struct MarketEvent {
    /// Live-feed identity.
    pub demo_run_id: String,
    /// Monotonic logical quote number.
    pub sequence: i64,
    /// Instrument identifier.
    pub symbol: String,
    /// Best bid price.
    pub bid_px: f64,
    /// Best ask price.
    pub ask_px: f64,
    /// Exchange-reported best-bid quantity.
    pub bid_qty: f64,
    /// Exchange-reported best-ask quantity.
    pub ask_qty: f64,
    /// Midpoint calculated by SQL.
    pub mid_px: f64,
    /// Spread calculated by SQL in basis points.
    pub spread_bps: f64,
    /// Live-feed label.
    pub regime: String,
    /// Event timestamp in microseconds since the Unix epoch.
    pub event_ts: i64,
}

/// Engine-produced fill presentation row.
#[derive(Debug, Clone, PartialEq, FromRow, Serialize)]
pub struct FillEvent {
    /// Browser-player identity.
    pub demo_run_id: String,
    /// Stable fill identity.
    pub fill_id: String,
    /// Quote sequence that caused this opportunity.
    pub source_event_id: i64,
    /// Strategy identifier.
    pub strategy: String,
    /// Instrument identifier.
    pub symbol: String,
    /// Simulated maker side.
    pub side: String,
    /// Simulated quantity.
    pub quantity: f64,
    /// Simulated fill price.
    pub fill_px: f64,
    /// Configured hypothetical fee in basis points.
    pub fee_bps: f64,
    /// Execution-time midpoint.
    pub mid_at_fill: f64,
    /// Execution-time spread from the input contract.
    pub spread_bps_at_fill: f64,
    /// Execution-time spread capture calculated by SQL.
    pub spread_capture_bps: f64,
    /// Visitor action explanation.
    pub decision_reason: String,
    /// Fill-model truthfulness label.
    pub fill_model: String,
    /// Event timestamp in microseconds since the Unix epoch.
    pub event_ts: i64,
}

/// Engine-produced long-form row for one fill and one horizon.
#[derive(Debug, Clone, PartialEq, FromRow, Serialize)]
pub struct MarkoutEvent {
    /// Browser-player identity.
    pub demo_run_id: String,
    /// Stable fill identity.
    pub fill_id: String,
    /// Strategy identifier.
    pub strategy: String,
    /// Instrument identifier.
    pub symbol: String,
    /// Simulated maker side.
    pub side: String,
    /// Simulated quantity.
    pub quantity: f64,
    /// Simulated fill price.
    pub fill_px: f64,
    /// Execution-time midpoint.
    pub mid_at_fill: f64,
    /// Probe offset in milliseconds.
    pub horizon_ms: i64,
    /// Probe timestamp in microseconds since the Unix epoch.
    pub probe_ts: i64,
    /// Matched quote timestamp, or null when uncovered.
    pub reference_quote_ts: Option<i64>,
    /// Matched future midpoint calculated by SQL, or null when uncovered.
    pub future_mid_px: Option<f64>,
    /// Execution-time spread capture in basis points.
    pub spread_capture_bps: f64,
    /// Post-fill midpoint movement in provider-signed basis points.
    pub post_fill_drift_bps: Option<f64>,
    /// Provider-signed gross markout in basis points.
    pub gross_markout_bps: Option<f64>,
    /// Provider-signed gross hypothetical PnL.
    pub gross_markout_pnl: Option<f64>,
    /// Configured hypothetical fee PnL.
    pub fee_pnl: f64,
    /// Gross plus fee hypothetical PnL.
    pub net_markout_pnl: Option<f64>,
    /// Age of the matched reference quote in milliseconds.
    pub quote_age_ms: Option<i64>,
    /// Whether the temporal probe found a reference quote.
    pub covered: bool,
    /// Visitor action explanation.
    pub decision_reason: String,
    /// Fill-model truthfulness label.
    pub fill_model: String,
}

/// Engine-produced strategy/horizon curve row.
#[derive(Debug, Clone, PartialEq, FromRow, Serialize)]
pub struct CurveEvent {
    /// Browser-player identity.
    pub demo_run_id: String,
    /// Strategy identifier.
    pub strategy: String,
    /// Probe offset in milliseconds.
    pub horizon_ms: i64,
    /// Notional-weighted execution-time spread capture calculated by SQL.
    pub weighted_spread_capture_bps: f64,
    /// Notional-weighted net markout calculated by SQL.
    pub weighted_net_markout_bps: f64,
    /// Aggregate net hypothetical PnL.
    pub total_net_markout_pnl: f64,
    /// Covered fill notional.
    pub filled_notional: f64,
    /// Covered fill count.
    pub covered_fills: i64,
    /// Share of covered fills with negative net markout, as a percentage.
    pub adverse_fill_rate: f64,
}

/// Engine-produced row containing the five primary dashboard KPIs for one strategy.
#[derive(Debug, Clone, PartialEq, FromRow, Serialize)]
pub struct SummaryEvent {
    /// Browser-player identity.
    pub demo_run_id: String,
    /// Strategy identifier.
    pub strategy: String,
    /// Horizon whose engine aggregate produced this nullable KPI update.
    pub horizon_ms: i64,
    /// Notional-weighted 0s spread capture, pending until available.
    pub spread_capture_0s_bps: Option<f64>,
    /// Notional-weighted 5s net markout, pending until available.
    pub weighted_markout_5s_bps: Option<f64>,
    /// Aggregate 30s net hypothetical PnL, pending until available.
    pub hypothetical_pnl_30s: Option<f64>,
    /// Execution-time covered notional, pending until the 0s aggregate is available.
    pub filled_notional: Option<f64>,
    /// 5s adverse fill percentage, pending until available.
    pub adverse_fill_rate_5s: Option<f64>,
}

/// Arrow schema for validated live-quote input.
#[must_use]
pub fn live_quote_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("sequence", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid_px", DataType::Float64, false),
        Field::new("ask_px", DataType::Float64, false),
        Field::new("bid_qty", DataType::Float64, false),
        Field::new("ask_qty", DataType::Float64, false),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Arrow schema for simulated-fill input.
#[must_use]
pub fn fill_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("demo_run_id", DataType::Utf8, false),
        Field::new("fill_id", DataType::Utf8, false),
        Field::new("source_event_id", DataType::Int64, false),
        Field::new("strategy", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("fill_px", DataType::Float64, false),
        Field::new("fee_bps", DataType::Float64, false),
        Field::new("mid_at_fill", DataType::Float64, false),
        Field::new("spread_bps_at_fill", DataType::Float64, false),
        Field::new("decision_reason", DataType::Utf8, false),
        Field::new("fill_model", DataType::Utf8, false),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Convert one validated live quote into a schema-checked Arrow batch.
///
/// # Errors
/// Returns an Arrow error when the columns cannot form the declared batch.
pub fn live_quote_to_batch(quote: &LiveQuote) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        live_quote_schema(),
        vec![
            Arc::new(StringArray::from(vec![quote.event_type.as_str()])),
            Arc::new(Int64Array::from(vec![quote.sequence])),
            Arc::new(StringArray::from(vec![quote.symbol.as_str()])),
            Arc::new(Float64Array::from(vec![quote.bid_px])),
            Arc::new(Float64Array::from(vec![quote.ask_px])),
            Arc::new(Float64Array::from(vec![quote.bid_qty])),
            Arc::new(Float64Array::from(vec![quote.ask_qty])),
            Arc::new(TimestampMicrosecondArray::from(vec![quote.event_ts])),
        ],
    )?)
}

/// Convert simulated fills to one schema-checked Arrow batch.
///
/// # Errors
/// Returns an Arrow error when the columns cannot form the declared batch.
pub fn fills_to_batch(fills: &[SimulatedFill]) -> Result<RecordBatch> {
    let batch = RecordBatch::try_new(
        fill_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.demo_run_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.fill_id.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                fills.iter().map(|fill| fill.source_event_id),
            )),
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.strategy.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.symbol.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.side.as_str()),
            )),
            Arc::new(Float64Array::from_iter_values(
                fills.iter().map(|fill| fill.quantity),
            )),
            Arc::new(Float64Array::from_iter_values(
                fills.iter().map(|fill| fill.fill_px),
            )),
            Arc::new(Float64Array::from_iter_values(
                fills.iter().map(|fill| fill.fee_bps),
            )),
            Arc::new(Float64Array::from_iter_values(
                fills.iter().map(|fill| fill.mid_at_fill),
            )),
            Arc::new(Float64Array::from_iter_values(
                fills.iter().map(|fill| fill.spread_bps_at_fill),
            )),
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.decision_reason.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                fills.iter().map(|fill| fill.fill_model.as_str()),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                fills.iter().map(|fill| fill.event_ts),
            )),
        ],
    )?;
    Ok(batch)
}
