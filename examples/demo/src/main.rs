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
//! # Observability
//!
//! Pipeline metrics are provided by `db.metrics()` (F-OBS-001):
//! - Event counters: `total_events_ingested`, `total_events_emitted`
//! - Cycle metrics: `total_cycles`, `last_cycle_duration_ns`
//! - Pipeline watermark: `pipeline_watermark`
//! - Source/stream backpressure: `db.source_metrics()`, `db.stream_metrics()`
//! - System stats (CPU/memory) use `sysinfo` directly.

use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::LaminarDB;

use laminardb_demo::app::App;
use laminardb_demo::asof_merge;
use laminardb_demo::generator::MarketGenerator;
use laminardb_demo::system_stats::StatsCollector;
use laminardb_demo::tui;
use laminardb_demo::types::{
    AnomalyAlert, BookImbalanceMetrics, DepthMetrics, MarketTick, OhlcBar, OrderBookUpdate,
    OrderEvent, SpreadMetrics, ViewMode, VolumeMetrics,
};

/// SQL files embedded at compile time.
const SOURCES_SQL: &str = include_str!("../sql/sources.sql");
const STREAMS_SQL: &str = include_str!("../sql/streams.sql");
const SINKS_SQL: &str = include_str!("../sql/sinks.sql");

#[cfg(feature = "kafka")]
const SOURCES_KAFKA_SQL: &str = include_str!("../sql/sources_kafka.sql");
#[cfg(feature = "kafka")]
const SINKS_KAFKA_SQL: &str = include_str!("../sql/sinks_kafka.sql");

// -- Shared abstractions --

/// Bundled subscription handles for all analytics streams.
struct Subscriptions {
    ohlc: laminar_db::TypedSubscription<OhlcBar>,
    volume: laminar_db::TypedSubscription<VolumeMetrics>,
    spread: laminar_db::TypedSubscription<SpreadMetrics>,
    anomaly: laminar_db::TypedSubscription<AnomalyAlert>,
    imbalance: laminar_db::TypedSubscription<BookImbalanceMetrics>,
    depth: laminar_db::TypedSubscription<DepthMetrics>,
}

impl Subscriptions {
    fn from_db(db: &LaminarDB) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            ohlc: db.subscribe::<OhlcBar>("ohlc_bars")?,
            volume: db.subscribe::<VolumeMetrics>("volume_metrics")?,
            spread: db.subscribe::<SpreadMetrics>("spread_metrics")?,
            anomaly: db.subscribe::<AnomalyAlert>("anomaly_alerts")?,
            imbalance: db.subscribe::<BookImbalanceMetrics>("book_imbalance")?,
            depth: db.subscribe::<DepthMetrics>("depth_metrics")?,
        })
    }
}

/// Abstraction over data generation and pipeline feeding.
///
/// `EmbeddedDataSource` pushes directly to in-memory sources;
/// `KafkaDataSource` produces to Redpanda topics.
trait PipelineDataSource {
    /// Generate and push one cycle of data into the pipeline.
    async fn push_cycle(&mut self, app: &mut App) -> Result<(), Box<dyn std::error::Error>>;
}

/// Embedded mode: synthetic data pushed via `SourceHandle`.
struct EmbeddedDataSource {
    generator: MarketGenerator,
    tick_source: laminar_db::SourceHandle<MarketTick>,
    order_source: laminar_db::SourceHandle<OrderEvent>,
    book_source: laminar_db::SourceHandle<OrderBookUpdate>,
}

impl PipelineDataSource for EmbeddedDataSource {
    async fn push_cycle(&mut self, app: &mut App) -> Result<(), Box<dyn std::error::Error>> {
        app.cycle += 1;
        let ts = chrono::Utc::now().timestamp_millis();

        let ticks = self.generator.generate_ticks(6, ts);
        let orders = self.generator.generate_orders(5, ts);
        let book_updates = self.generator.generate_book_updates(ts);

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
        let enriched = asof_merge::merge_orders_with_ticks(&order_tuples, &app.tick_buffer);
        app.ingest_enriched_orders(enriched);

        // Apply book updates to in-memory L2 book
        app.apply_book_updates(&book_updates);

        // Cleanup old ticks periodically
        app.cleanup_tick_buffer(ts);

        app.total_ticks += self.tick_source.push_batch(ticks) as u64;
        app.total_orders += self.order_source.push_batch(orders) as u64;
        self.book_source.push_batch(book_updates);

        // Advance watermarks
        self.tick_source.watermark(ts + 5_000);
        self.order_source.watermark(ts + 5_000);
        self.book_source.watermark(ts + 5_000);

        Ok(())
    }
}

/// Kafka mode: synthetic data produced to Redpanda topics.
#[cfg(feature = "kafka")]
struct KafkaDataSource {
    generator: MarketGenerator,
    producer: rdkafka::producer::FutureProducer,
}

#[cfg(feature = "kafka")]
impl PipelineDataSource for KafkaDataSource {
    async fn push_cycle(&mut self, app: &mut App) -> Result<(), Box<dyn std::error::Error>> {
        use laminardb_demo::generator;

        app.cycle += 1;
        let ts = chrono::Utc::now().timestamp_millis();

        let ticks = self.generator.generate_kafka_ticks(6, ts);
        let orders = self.generator.generate_kafka_orders(5, ts);
        let book_updates = self.generator.generate_kafka_book_updates(ts);

        // Update app-level state from generated data
        let app_ticks: Vec<_> = ticks.iter().map(|t| t.to_market_tick()).collect();
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
        let enriched = asof_merge::merge_orders_with_ticks(&order_tuples, &app.tick_buffer);
        app.ingest_enriched_orders(enriched);
        app.cleanup_tick_buffer(ts);

        // Produce to Kafka topics (keyed by symbol for co-partitioning)
        if let Err(e) =
            generator::produce_to_kafka_keyed(&self.producer, "market-ticks", &ticks, |t| {
                Some(&t.symbol)
            })
            .await
        {
            eprintln!("[kafka] produce error (market-ticks): {e}");
        }
        if let Err(e) =
            generator::produce_to_kafka_keyed(&self.producer, "order-events", &orders, |o| {
                Some(&o.symbol)
            })
            .await
        {
            eprintln!("[kafka] produce error (order-events): {e}");
        }
        if let Err(e) =
            generator::produce_to_kafka_keyed(&self.producer, "book-updates", &book_updates, |b| {
                Some(&b.symbol)
            })
            .await
        {
            eprintln!("[kafka] produce error (book-updates): {e}");
        }

        Ok(())
    }
}

// -- Main --

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("DEMO_MODE").unwrap_or_else(|_| "embedded".into());

    match mode.as_str() {
        #[cfg(feature = "kafka")]
        "kafka" => run_kafka_mode().await,
        _ => run_embedded_mode().await,
    }
}

/// Embedded mode: in-memory sources, continuous data generation, Ratatui dashboard.
async fn run_embedded_mode() -> Result<(), Box<dyn std::error::Error>> {
    // -- Build LaminarDB --
    let ckpt_dir = std::env::temp_dir().join("laminardb-demo-ckpt");
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", "localhost:19092")
        .config_var("GROUP_ID", "demo")
        .config_var("ENVIRONMENT", "development")
        .buffer_size(65536)
        .checkpoint(StreamCheckpointConfig {
            data_dir: Some(ckpt_dir),
            interval_ms: None, // manual only via [c]
            max_retained: Some(5),
            ..StreamCheckpointConfig::default()
        })
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
    let subs = Subscriptions::from_db(&db)?;

    let mut generator = MarketGenerator::new();
    let mut app = App::new();
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

    // -- Run TUI with embedded data source --
    let mut data_source = EmbeddedDataSource {
        generator,
        tick_source,
        order_source,
        book_source,
    };

    let result = run_with_tui(&mut app, &mut data_source, &subs, &db).await;

    // -- Shutdown --
    db.shutdown().await?;
    result
}

// -- Kafka mode --

#[cfg(feature = "kafka")]
async fn run_kafka_mode() -> Result<(), Box<dyn std::error::Error>> {
    use laminardb_demo::generator;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::FutureProducer;

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    let group_id = std::env::var("GROUP_ID").unwrap_or_else(|_| "laminardb-demo".into());

    // -- Build LaminarDB with Kafka config --
    let ckpt_dir = std::env::temp_dir().join("laminardb-demo-kafka-ckpt");
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", &brokers)
        .config_var("GROUP_ID", &group_id)
        .config_var("ENVIRONMENT", "kafka-demo")
        .buffer_size(65536)
        .checkpoint(StreamCheckpointConfig {
            data_dir: Some(ckpt_dir),
            interval_ms: None,
            max_retained: Some(5),
            ..StreamCheckpointConfig::default()
        })
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
        generator::produce_to_kafka_keyed(&producer, "market-ticks", &ticks, |t| Some(&t.symbol))
            .await?;
    let order_count =
        generator::produce_to_kafka_keyed(&producer, "order-events", &orders, |o| Some(&o.symbol))
            .await?;
    let book_count =
        generator::produce_to_kafka_keyed(&producer, "book-updates", &book_updates, |b| {
            Some(&b.symbol)
        })
        .await?;

    eprintln!(
        "Produced {} ticks, {} orders, {} book updates to Kafka",
        tick_count, order_count, book_count
    );

    // -- Subscribe and setup app state --
    let subs = Subscriptions::from_db(&db)?;

    let mut app = App::new();
    app.set_topology(db.pipeline_topology());

    // Seed app state from initial batch
    let app_ticks: Vec<_> = ticks.iter().map(|t| t.to_market_tick()).collect();
    let app_book: Vec<_> = book_updates
        .iter()
        .map(|b| b.to_order_book_update())
        .collect();
    app.ingest_ticks_for_asof(&app_ticks);
    app.apply_book_updates(&app_book);
    app.total_ticks += tick_count as u64;
    app.total_orders += order_count as u64;

    // -- Run TUI with Kafka data source --
    let mut data_source = KafkaDataSource {
        generator: gen,
        producer,
    };

    let result = run_with_tui(&mut app, &mut data_source, &subs, &db).await;

    // -- Shutdown --
    db.shutdown().await?;
    result
}

// -- Shared TUI event loop --

/// Setup terminal, run the TUI event loop, and restore terminal on exit.
async fn run_with_tui<D: PipelineDataSource>(
    app: &mut App,
    data_source: &mut D,
    subs: &Subscriptions,
    db: &LaminarDB,
) -> Result<(), Box<dyn std::error::Error>> {
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

    let mut stats_collector = StatsCollector::new();
    let result = run_tui_loop(
        &mut terminal,
        app,
        &mut stats_collector,
        data_source,
        subs,
        db,
    )
    .await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

/// Shared TUI event loop: render, handle input, push data, drain subscriptions.
async fn run_tui_loop<D: PipelineDataSource>(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    stats_collector: &mut StatsCollector,
    data_source: &mut D,
    subs: &Subscriptions,
    db: &LaminarDB,
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
                        KeyCode::Char('c') => {
                            if let Ok(Some(epoch)) = db.checkpoint() {
                                app.record_checkpoint(epoch);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }

        // Update system stats and pipeline metrics every cycle (even when paused)
        app.update_system_stats(stats_collector.refresh());
        app.update_pipeline_metrics(db.metrics());

        if !app.paused {
            data_source.push_cycle(app).await?;
            drain_subscriptions(app, subs);
        }
    }

    Ok(())
}

/// Poll all subscription channels and merge results into app state.
fn drain_subscriptions(app: &mut App, subs: &Subscriptions) {
    for _ in 0..64 {
        match subs.ohlc.poll() {
            Some(rows) => app.ingest_ohlc(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match subs.volume.poll() {
            Some(rows) => app.ingest_volume(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match subs.spread.poll() {
            Some(rows) => app.ingest_spread(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match subs.anomaly.poll() {
            Some(rows) => app.ingest_anomaly(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match subs.imbalance.poll() {
            Some(rows) => app.ingest_book_imbalance(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match subs.depth.poll() {
            Some(rows) => app.ingest_depth_metrics(rows),
            None => break,
        }
    }
}
