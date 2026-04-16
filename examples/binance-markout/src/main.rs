//! Mark-out analysis demo harness.

use std::time::{Duration, Instant};

use laminar_db::{LaminarDB, TypedSubscription};
use laminar_derive::FromRow;

#[derive(Debug, Clone, FromRow)]
#[allow(dead_code)]
struct MarkoutProbe {
    s: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_appender = tracing_appender::rolling::never(".", "binance-markout.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Tuned for BTC-USDT-PERP bookTicker (~500 evt/s).
    let db = LaminarDB::builder()
        .pipeline_batch_window(Duration::from_millis(20))
        .pipeline_query_budget_ns(40_000_000)
        .pipeline_max_input_buf_batches(1024)
        .build()
        .await?;
    db.execute(include_str!("../pipeline.sql")).await?;
    db.start().await?;

    let mut markouts_sub: TypedSubscription<MarkoutProbe> = db.subscribe("markouts_long")?;

    println!("binance-markout running — connecting to Binance USDT-M futures");
    println!("  trades: wss://fstream.binance.com/ws/btcusdt@trade");
    println!("  book:   wss://fstream.binance.com/ws/btcusdt@bookTicker");
    println!();
    println!("Dashboard WebSocket endpoints:");
    println!("  ws://127.0.0.1:9001/markouts   (per-trade, 5/15/30/60s horizons)");
    println!("  ws://127.0.0.1:9002/curve      (1-min per-side markout curve)");
    println!("  ws://127.0.0.1:9003/toxicity   (30s per-horizon metrics)");
    println!("  ws://127.0.0.1:9004/alerts     (adverse-selection alerts)");
    println!("  ws://127.0.0.1:9005/regime     (1-min per-side flow regime)");
    println!("  ws://127.0.0.1:9006/signal     (30s MM quote-skew signal)");
    println!();
    println!("Press Ctrl-C to stop.");
    println!();

    let mut last_total = 0u64;
    let mut last_tick = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut markouts_seen: u64 = 0;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // Drain the direct Rust subscription to count markouts_long rows.
                while let Some(rows) = markouts_sub.poll() {
                    markouts_seen += rows.len() as u64;
                }
                let m = db.metrics();
                let elapsed = last_tick.elapsed().as_secs_f64();
                let rate = (m.total_events_ingested.saturating_sub(last_total)) as f64 / elapsed;
                println!(
                    "events total={:>8}  rate={:>6.1}/s  markouts={:>6}  sources={}  streams={}",
                    m.total_events_ingested, rate, markouts_seen, m.source_count, m.stream_count,
                );
                last_total = m.total_events_ingested;
                last_tick = Instant::now();
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nshutting down...");
                break;
            }
        }
    }

    db.shutdown().await?;
    Ok(())
}
