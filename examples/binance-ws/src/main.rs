#![allow(clippy::disallowed_types)]

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
use crate::types::{Signal, Vwap, SYMBOLS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Log to file (keeps TUI clean)
    let file_appender = tracing_appender::rolling::never(".", "binance-ws.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // ── Step 1: Open database ────────────────────────────────────
    let db = LaminarDB::open()?;

    // ── Step 2: Load SQL pipeline ────────────────────────────────
    db.execute(include_str!("../pipeline.sql")).await?;

    // ── Step 3: Start processing ─────────────────────────────────
    db.start().await?;
    std::thread::sleep(Duration::from_secs(2));

    // ── Step 4: Subscribe to all streams ─────────────────────────
    let mut vwap_subs: Vec<TypedSubscription<Vwap>> = Vec::new();
    let mut signal_subs: Vec<TypedSubscription<Signal>> = Vec::new();
    for sym in SYMBOLS {
        vwap_subs.push(db.subscribe::<Vwap>(&format!("vwap_{sym}"))?);
        signal_subs.push(db.subscribe::<Signal>(&format!("signals_{sym}"))?);
    }

    // ── Step 5: Run dashboard ────────────────────────────────────
    let mut state = DashState {
        vwap: HashMap::new(),
        signals: HashMap::new(),
        start: Instant::now(),
        total_events: 0,
        throughput: 0.0,
        cycle_ns: 0,
        source_count: 0,
        stream_count: 0,
    };
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

        drain_subs(&vwap_subs, &mut state.vwap, |r| r.symbol.clone());
        drain_subs(&signal_subs, &mut state.signals, |r| r.symbol.clone());

        let m = db.metrics();
        state.total_events = m.total_events_ingested;
        state.cycle_ns = m.last_cycle_duration_ns;
        state.source_count = m.source_count;
        state.stream_count = m.stream_count;
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

fn drain_subs<T: Clone + FromBatch>(
    subs: &[TypedSubscription<T>],
    map: &mut HashMap<String, T>,
    key_fn: fn(&T) -> String,
) {
    for sub in subs {
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
}
