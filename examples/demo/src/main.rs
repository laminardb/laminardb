//! LaminarDB Market Data Demo.
//!
//! A production-style demo showcasing real-time market data analytics with a
//! Ratatui TUI dashboard. Demonstrates LaminarDB's streaming SQL capabilities:
//! OHLC bars, order flow analysis, spread tracking, anomaly detection,
//! L2 order book simulation, and system resource monitoring.
//!
//! # Running
//!
//! ## Embedded mode (default, no external dependencies):
//! ```bash
//! cargo run -p laminardb-demo
//! ```
//!
//! ## Kafka mode (requires Docker):
//! ```bash
//! docker-compose -f examples/demo/docker-compose.yml up -d
//! bash examples/demo/scripts/setup-kafka.sh
//! DEMO_MODE=kafka cargo run -p laminardb-demo --features kafka
//! ```
//!
//! # Core API Gaps
//!
//! The following observability APIs are not yet available in LaminarDB:
//! - **No system metrics API**: `LaminarDB` has no `db.metrics()` for CPU/memory/latency.
//!   Workaround: use `sysinfo` crate directly.
//! - **No pipeline latency tracking**: No way to measure push-to-poll end-to-end latency.
//!   Workaround: manual timestamp comparison.
//! - **No event counters**: No `db.total_events_processed()`.
//!   Workaround: track `total_ticks` manually in app.
//! - **No per-stream metrics**: Can't query `db.stream_stats("ohlc_bars")` for
//!   throughput/backpressure. Workaround: not available.
//! - **No backpressure feedback**: `push_batch()` silently drops when channel full;
//!   no `is_backpressured()`. Workaround: accept silent drops.
//! - **No watermark visibility**: Can't observe internal watermark state of streams.
//! - **No queue depth introspection**: Only `source.pending()` exists; no sink/stream
//!   queue depth.

use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use laminar_db::LaminarDB;

use laminardb_demo::app::App;
use laminardb_demo::asof_merge;
use laminardb_demo::generator::MarketGenerator;
use laminardb_demo::system_stats::StatsCollector;
use laminardb_demo::types::{
    AnomalyAlert, BookImbalanceMetrics, DepthMetrics, MarketTick, OhlcBar,
    OrderBookUpdate, OrderEvent, SpreadMetrics, ViewMode, VolumeMetrics,
};
use laminardb_demo::tui;

/// SQL files embedded at compile time.
const SOURCES_SQL: &str = include_str!("../sql/sources.sql");
const STREAMS_SQL: &str = include_str!("../sql/streams.sql");
const SINKS_SQL: &str = include_str!("../sql/sinks.sql");

#[cfg(feature = "kafka")]
const SOURCES_KAFKA_SQL: &str = include_str!("../sql/sources_kafka.sql");
#[cfg(feature = "kafka")]
const SINKS_KAFKA_SQL: &str = include_str!("../sql/sinks_kafka.sql");

// -- Main --

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode =
        std::env::var("DEMO_MODE").unwrap_or_else(|_| "embedded".into());

    match mode.as_str() {
        #[cfg(feature = "kafka")]
        "kafka" => run_kafka_mode().await,
        _ => run_embedded_mode().await,
    }
}

/// Embedded mode: in-memory sources, continuous data generation, Ratatui dashboard.
async fn run_embedded_mode() -> Result<(), Box<dyn std::error::Error>> {
    // -- Build LaminarDB --
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", "localhost:9092")
        .config_var("GROUP_ID", "demo")
        .config_var("ENVIRONMENT", "development")
        .buffer_size(65536)
        .build()
        .await?;

    // -- Execute SQL pipelines --
    db.execute(SOURCES_SQL).await?;
    db.execute(STREAMS_SQL).await?;
    db.execute(SINKS_SQL).await?;
    db.start().await?;

    // -- Acquire typed handles --
    let tick_source = db.source::<MarketTick>("market_ticks")?;
    let order_source = db.source::<OrderEvent>("order_events")?;
    let book_source = db.source::<OrderBookUpdate>("book_updates")?;

    let ohlc_sub = db.subscribe::<OhlcBar>("ohlc_bars")?;
    let volume_sub = db.subscribe::<VolumeMetrics>("volume_metrics")?;
    let spread_sub = db.subscribe::<SpreadMetrics>("spread_metrics")?;
    let anomaly_sub = db.subscribe::<AnomalyAlert>("anomaly_alerts")?;
    let imbalance_sub =
        db.subscribe::<BookImbalanceMetrics>("book_imbalance")?;
    let depth_sub = db.subscribe::<DepthMetrics>("depth_metrics")?;

    // -- Initialize generator, stats, and app state --
    let mut generator = MarketGenerator::new();
    let mut stats_collector = StatsCollector::new();
    let mut app = App::new();

    // Capture pipeline topology for DAG view
    app.set_topology(db.pipeline_topology());

    // Push initial batch so there's data right away
    let ts = chrono::Utc::now().timestamp_millis();
    let ticks = generator.generate_ticks(10, ts);
    let orders = generator.generate_orders(5, ts);
    let book_updates = generator.generate_book_updates(ts);
    app.total_ticks += tick_source.push_batch(ticks) as u64;
    app.total_orders += order_source.push_batch(orders) as u64;
    app.apply_book_updates(&book_updates);
    book_source.push_batch(book_updates);
    tick_source.watermark(ts + 5_000);
    order_source.watermark(ts + 5_000);
    book_source.watermark(ts + 5_000);

    // -- Setup terminal --
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Install panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(panic_info);
    }));

    // -- Event loop (5 FPS) --
    let result = run_loop(
        &mut terminal,
        &mut app,
        &mut generator,
        &mut stats_collector,
        &tick_source,
        &order_source,
        &book_source,
        &ohlc_sub,
        &volume_sub,
        &spread_sub,
        &anomaly_sub,
        &imbalance_sub,
        &depth_sub,
    );

    // -- Restore terminal --
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    // -- Shutdown --
    db.shutdown().await?;

    result
}

/// Main event loop: render, handle input, push data, drain subscriptions.
#[allow(clippy::too_many_arguments)]
fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    generator: &mut MarketGenerator,
    stats_collector: &mut StatsCollector,
    tick_source: &laminar_db::SourceHandle<MarketTick>,
    order_source: &laminar_db::SourceHandle<OrderEvent>,
    book_source: &laminar_db::SourceHandle<OrderBookUpdate>,
    ohlc_sub: &laminar_db::TypedSubscription<OhlcBar>,
    volume_sub: &laminar_db::TypedSubscription<VolumeMetrics>,
    spread_sub: &laminar_db::TypedSubscription<SpreadMetrics>,
    anomaly_sub: &laminar_db::TypedSubscription<AnomalyAlert>,
    imbalance_sub: &laminar_db::TypedSubscription<BookImbalanceMetrics>,
    depth_sub: &laminar_db::TypedSubscription<DepthMetrics>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        // Render
        terminal.draw(|f| tui::draw(f, app))?;

        // Handle input (200ms poll = 5 FPS)
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            app.should_quit = true;
                        }
                        KeyCode::Tab => {
                            app.next_symbol();
                        }
                        KeyCode::Char(' ') => {
                            app.paused = !app.paused;
                        }
                        KeyCode::Char('b') => {
                            app.set_or_toggle_view(ViewMode::OrderBook);
                        }
                        KeyCode::Char('d') => {
                            app.set_or_toggle_view(ViewMode::Dag);
                        }
                        _ => {}
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }

        // Update system stats every cycle (even when paused)
        app.update_system_stats(stats_collector.refresh());

        if !app.paused {
            app.cycle += 1;
            let ts = chrono::Utc::now().timestamp_millis();

            // Generate and push data
            let ticks = generator.generate_ticks(6, ts);
            let orders = generator.generate_orders(5, ts);
            let book_updates = generator.generate_book_updates(ts);

            // Buffer raw ticks for ASOF matching
            app.ingest_ticks_for_asof(&ticks);

            // Run ASOF merge: enrich orders with latest market data
            let order_tuples: Vec<_> = orders
                .iter()
                .map(|o| {
                    (
                        o.order_id.clone(),
                        o.symbol.clone(),
                        o.side.clone(),
                        o.quantity,
                        o.price,
                        o.ts,
                    )
                })
                .collect();
            let enriched = asof_merge::merge_orders_with_ticks(
                &order_tuples,
                &app.tick_buffer,
            );
            app.ingest_enriched_orders(enriched);

            // Apply book updates to in-memory L2 book
            app.apply_book_updates(&book_updates);

            // Cleanup old ticks periodically
            app.cleanup_tick_buffer(ts);

            app.total_ticks += tick_source.push_batch(ticks) as u64;
            app.total_orders += order_source.push_batch(orders) as u64;
            book_source.push_batch(book_updates);

            // Advance watermarks
            tick_source.watermark(ts + 5_000);
            order_source.watermark(ts + 5_000);
            book_source.watermark(ts + 5_000);

            // Drain all subscriptions
            drain_subscriptions(
                app,
                ohlc_sub,
                volume_sub,
                spread_sub,
                anomaly_sub,
                imbalance_sub,
                depth_sub,
            );
        }
    }

    Ok(())
}

/// Poll all subscription channels and merge results into app state.
#[allow(clippy::too_many_arguments)]
fn drain_subscriptions(
    app: &mut App,
    ohlc_sub: &laminar_db::TypedSubscription<OhlcBar>,
    volume_sub: &laminar_db::TypedSubscription<VolumeMetrics>,
    spread_sub: &laminar_db::TypedSubscription<SpreadMetrics>,
    anomaly_sub: &laminar_db::TypedSubscription<AnomalyAlert>,
    imbalance_sub: &laminar_db::TypedSubscription<BookImbalanceMetrics>,
    depth_sub: &laminar_db::TypedSubscription<DepthMetrics>,
) {
    for _ in 0..64 {
        match ohlc_sub.poll() {
            Some(rows) => app.ingest_ohlc(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match volume_sub.poll() {
            Some(rows) => app.ingest_volume(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match spread_sub.poll() {
            Some(rows) => app.ingest_spread(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match anomaly_sub.poll() {
            Some(rows) => app.ingest_anomaly(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match imbalance_sub.poll() {
            Some(rows) => app.ingest_book_imbalance(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match depth_sub.poll() {
            Some(rows) => app.ingest_depth_metrics(rows),
            None => break,
        }
    }
}

// -- Kafka mode --

#[cfg(feature = "kafka")]
async fn run_kafka_mode() -> Result<(), Box<dyn std::error::Error>> {
    use laminardb_demo::generator;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::FutureProducer;

    let brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "localhost:9092".into());
    let group_id = std::env::var("GROUP_ID")
        .unwrap_or_else(|_| "laminardb-demo".into());

    // -- Build LaminarDB with Kafka config --
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", &brokers)
        .config_var("GROUP_ID", &group_id)
        .config_var("ENVIRONMENT", "kafka-demo")
        .buffer_size(65536)
        .build()
        .await?;

    db.execute(SOURCES_KAFKA_SQL).await?;
    db.execute(STREAMS_SQL).await?;
    db.execute(SINKS_KAFKA_SQL).await?;
    db.start().await?;

    // -- Produce initial data to Kafka --
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let mut gen = MarketGenerator::new();
    let base_ts = chrono::Utc::now().timestamp_millis();

    let ticks = gen.generate_kafka_ticks(20, base_ts);
    let orders = gen.generate_kafka_orders(10, base_ts);
    let book_updates = gen.generate_kafka_book_updates(base_ts);
    let tick_count =
        generator::produce_to_kafka(&producer, "market-ticks", &ticks)
            .await?;
    let order_count =
        generator::produce_to_kafka(&producer, "order-events", &orders)
            .await?;
    let book_count = generator::produce_to_kafka(
        &producer,
        "book-updates",
        &book_updates,
    )
    .await?;

    eprintln!(
        "Produced {} ticks, {} orders, {} book updates to Kafka",
        tick_count, order_count, book_count
    );

    // -- Subscribe and run TUI --
    let ohlc_sub = db.subscribe::<OhlcBar>("ohlc_bars")?;
    let volume_sub = db.subscribe::<VolumeMetrics>("volume_metrics")?;
    let spread_sub = db.subscribe::<SpreadMetrics>("spread_metrics")?;
    let anomaly_sub = db.subscribe::<AnomalyAlert>("anomaly_alerts")?;
    let imbalance_sub =
        db.subscribe::<BookImbalanceMetrics>("book_imbalance")?;
    let depth_sub = db.subscribe::<DepthMetrics>("depth_metrics")?;

    let mut app = App::new();
    let mut stats_collector = StatsCollector::new();
    app.set_topology(db.pipeline_topology());

    // Seed app state from initial batch
    let app_ticks: Vec<_> =
        ticks.iter().map(|t| t.to_market_tick()).collect();
    let app_book: Vec<_> = book_updates
        .iter()
        .map(|b| b.to_order_book_update())
        .collect();
    app.ingest_ticks_for_asof(&app_ticks);
    app.apply_book_updates(&app_book);
    app.total_ticks += tick_count as u64;
    app.total_orders += order_count as u64;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(panic_info);
    }));

    loop {
        terminal.draw(|f| tui::draw(f, &app))?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => break,
                        KeyCode::Tab => app.next_symbol(),
                        KeyCode::Char(' ') => app.paused = !app.paused,
                        KeyCode::Char('b') => {
                            app.set_or_toggle_view(ViewMode::OrderBook)
                        }
                        KeyCode::Char('d') => {
                            app.set_or_toggle_view(ViewMode::Dag)
                        }
                        _ => {}
                    }
                }
            }
        }

        app.update_system_stats(stats_collector.refresh());

        if !app.paused {
            app.cycle += 1;

            // Produce data to Kafka and update local app state
            let ts = chrono::Utc::now().timestamp_millis();
            let ticks = gen.generate_kafka_ticks(6, ts);
            let orders = gen.generate_kafka_orders(5, ts);
            let book_updates = gen.generate_kafka_book_updates(ts);

            // Update app-level state from generated data
            let app_ticks: Vec<_> =
                ticks.iter().map(|t| t.to_market_tick()).collect();
            let app_book: Vec<_> = book_updates
                .iter()
                .map(|b| b.to_order_book_update())
                .collect();

            app.ingest_ticks_for_asof(&app_ticks);
            app.apply_book_updates(&app_book);
            app.total_ticks += ticks.len() as u64;
            app.total_orders += orders.len() as u64;

            // ASOF merge: enrich orders with latest market data
            let order_tuples: Vec<_> = orders
                .iter()
                .map(|o| {
                    (
                        o.order_id.clone(),
                        o.symbol.clone(),
                        o.side.clone(),
                        o.quantity,
                        o.price,
                        o.ts,
                    )
                })
                .collect();
            let enriched = asof_merge::merge_orders_with_ticks(
                &order_tuples,
                &app.tick_buffer,
            );
            app.ingest_enriched_orders(enriched);
            app.cleanup_tick_buffer(ts);

            // Produce to Kafka topics
            let _ = generator::produce_to_kafka(
                &producer,
                "market-ticks",
                &ticks,
            )
            .await;
            let _ = generator::produce_to_kafka(
                &producer,
                "order-events",
                &orders,
            )
            .await;
            let _ = generator::produce_to_kafka(
                &producer,
                "book-updates",
                &book_updates,
            )
            .await;

            // Drain pipeline output subscriptions
            drain_subscriptions(
                &mut app,
                &ohlc_sub,
                &volume_sub,
                &spread_sub,
                &anomaly_sub,
                &imbalance_sub,
                &depth_sub,
            );
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    db.shutdown().await?;
    Ok(())
}
