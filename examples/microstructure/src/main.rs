#![allow(clippy::disallowed_types)]

mod history;
mod latency;
mod tui;
mod types;

use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use laminar_db::{FromBatch, LaminarDB, TypedSubscription};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use crate::tui::DashState;
use crate::types::{Burst, DepthLevel, MicroCandle, Momentum, Signal, Spread, Vwap};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_appender = tracing_appender::rolling::never(".", "microstructure.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db = LaminarDB::open()?;
    db.execute(include_str!("../pipeline.sql")).await?;
    db.start().await?;
    std::thread::sleep(Duration::from_secs(2));

    // ── Subscribe to 8 unified streams + 2 depth streams ─────
    let spread_sub = db.subscribe::<Spread>("spreads")?;
    let vwap_sub = db.subscribe::<Vwap>("vwap")?;
    let micro_sub = db.subscribe::<MicroCandle>("micro")?;
    let momentum_sub = db.subscribe::<Momentum>("momentum")?;
    let burst_sub = db.subscribe::<Burst>("bursts")?;
    let signal_sub = db.subscribe::<Signal>("signals")?;
    let depth_bids_sub = db.subscribe::<DepthLevel>("depth_bids")?;
    let depth_asks_sub = db.subscribe::<DepthLevel>("depth_asks")?;

    let mut state = DashState::new();
    let mut last_events = 0u64;
    let mut last_tp_time = Instant::now();

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    loop {
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press
                    && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                {
                    break;
                }
            }
        }

        drain_sub(&spread_sub, &mut state.spreads, |r| r.symbol.clone());
        drain_sub(&vwap_sub, &mut state.vwap, |r| r.symbol.clone());
        drain_sub(&momentum_sub, &mut state.momentum, |r| r.symbol.clone());
        drain_sub(&burst_sub, &mut state.bursts, |r| r.symbol.clone());
        drain_sub(&signal_sub, &mut state.signals, |r| r.symbol.clone());
        drain_micro(&micro_sub, &mut state);
        drain_depth(&depth_bids_sub, &mut state.depth_bids);
        drain_depth(&depth_asks_sub, &mut state.depth_asks);

        let m = db.metrics();
        state.total_events = m.total_events_ingested;
        state.total_emitted = m.total_events_emitted;
        state.total_dropped = m.total_events_dropped;
        state.source_count = m.source_count;
        state.stream_count = m.stream_count;
        state.pipeline_watermark = m.pipeline_watermark;
        state.latency.record(m.last_cycle_duration_ns);
        state.latency_history.push(m.last_cycle_duration_ns);

        let elapsed = last_tp_time.elapsed().as_secs_f64();
        if elapsed >= 1.0 {
            state.throughput = state.total_events.saturating_sub(last_events) as f64 / elapsed;
            last_events = state.total_events;
            last_tp_time = Instant::now();
        }

        terminal.draw(|f| tui::render(f, &state))?;
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    db.shutdown().await?;
    Ok(())
}

fn drain_sub<T: Clone + FromBatch>(
    sub: &TypedSubscription<T>,
    map: &mut HashMap<String, T>,
    key_fn: fn(&T) -> String,
) {
    for _ in 0..64 {
        match sub.poll() {
            Some(rows) => {
                for r in rows {
                    map.insert(key_fn(&r), r);
                }
            }
            None => break,
        }
    }
}

fn drain_micro(sub: &TypedSubscription<MicroCandle>, state: &mut DashState) {
    for _ in 0..64 {
        match sub.poll() {
            Some(rows) => {
                for r in rows {
                    let key = r.symbol.clone();
                    state
                        .price_history
                        .entry(key.clone())
                        .or_insert_with(|| crate::history::RingBuf::new(60))
                        .push(r.vwap);
                    state.micro_candles.insert(key, r);
                }
            }
            None => break,
        }
    }
}

fn drain_depth(sub: &TypedSubscription<DepthLevel>, buf: &mut Vec<DepthLevel>) {
    for _ in 0..64 {
        match sub.poll() {
            Some(rows) => {
                buf.clear();
                buf.extend(rows);
            }
            None => break,
        }
    }
}
