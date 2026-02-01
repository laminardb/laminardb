//! ShopStream: E-Commerce Analytics Demo for LaminarDB.
//!
//! Demonstrates all Phase 3 features in a realistic e-commerce scenario:
//!
//! - **F-DB-001**: Builder API with config vars
//! - **F-SQL-005**: Multi-statement SQL execution
//! - **F-SQL-006**: Config variable `${VAR}` substitution
//! - **F-SQL-001**: Extended Source DDL (FROM KAFKA syntax)
//! - **F-SQL-002**: Extended Sink DDL (INTO KAFKA + WHERE)
//! - **F-SQL-003**: CREATE STREAM named pipelines
//! - **F-SQL-004**: CREATE TABLE + INSERT INTO
//! - **F-DERIVE-001**: Record + FromRecordBatch derive macros
//! - **F-DB-002**: Named stream subscriptions
//! - **F-DB-003**: Pipeline lifecycle (start/shutdown)
//! - **F-DB-004**: Connector Manager
//!
//! # Running
//!
//! ## Embedded mode (default, no external dependencies):
//! ```bash
//! cargo run -p shopstream-demo
//! ```
//!
//! ## Kafka mode (requires Docker):
//! ```bash
//! docker-compose -f examples/shopstream/docker-compose.yml up -d
//! bash examples/shopstream/scripts/setup-kafka.sh
//! SHOPSTREAM_MODE=kafka cargo run -p shopstream-demo --features kafka
//! ```

mod generator;
mod types;

use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use laminar_db::{ExecuteResult, LaminarDB};

use types::{
    ClickEvent, InventoryUpdate, OrderEvent, OrderTotals, ProductActivity,
    SessionActivity,
};

/// SQL files embedded at compile time.
const TABLES_SQL: &str = include_str!("../sql/tables.sql");
const SOURCES_SQL: &str = include_str!("../sql/sources.sql");
const STREAMS_SQL: &str = include_str!("../sql/streams.sql");
const SINKS_SQL: &str = include_str!("../sql/sinks.sql");

/// Kafka-mode SQL files (richer schemas, FROM KAFKA / INTO KAFKA).
#[cfg(feature = "kafka")]
const SOURCES_KAFKA_SQL: &str = include_str!("../sql/sources_kafka.sql");
#[cfg(feature = "kafka")]
const STREAMS_KAFKA_SQL: &str = include_str!("../sql/streams_kafka.sql");
#[cfg(feature = "kafka")]
const SINKS_KAFKA_SQL: &str = include_str!("../sql/sinks_kafka.sql");

// ── Dashboard state ─────────────────────────────────────────────────

/// Accumulated dashboard state for the live TUI display.
struct DashboardState {
    // Counters
    total_clicks: u64,
    total_orders: u64,
    total_inventory: u64,
    cycle: u64,

    // Latest stream results
    sessions: Vec<SessionActivity>,
    order_totals: Vec<OrderTotals>,
    product_activity: HashMap<String, ProductActivityRow>,
}

/// Merged product activity across event types.
struct ProductActivityRow {
    product_id: String,
    views: i64,
    carts: i64,
    checkouts: i64,
    total: i64,
}

impl DashboardState {
    fn new() -> Self {
        Self {
            total_clicks: 0,
            total_orders: 0,
            total_inventory: 0,
            cycle: 0,
            sessions: Vec::new(),
            order_totals: Vec::new(),
            product_activity: HashMap::new(),
        }
    }

    fn ingest_sessions(&mut self, rows: Vec<SessionActivity>) {
        for row in rows {
            // Update or insert: keep latest per session_id
            if let Some(existing) = self
                .sessions
                .iter_mut()
                .find(|s| s.session_id == row.session_id)
            {
                *existing = row;
            } else {
                self.sessions.push(row);
            }
        }
        // Keep most active sessions at the top
        self.sessions
            .sort_by(|a, b| b.event_count.cmp(&a.event_count));
        self.sessions.truncate(10);
    }

    fn ingest_order_totals(&mut self, rows: Vec<OrderTotals>) {
        for row in rows {
            if let Some(existing) = self
                .order_totals
                .iter_mut()
                .find(|o| o.user_id == row.user_id)
            {
                *existing = row;
            } else {
                self.order_totals.push(row);
            }
        }
        self.order_totals
            .sort_by(|a, b| b.gross_revenue.partial_cmp(&a.gross_revenue).unwrap());
        self.order_totals.truncate(10);
    }

    fn ingest_product_activity(&mut self, rows: Vec<ProductActivity>) {
        for row in rows {
            let entry = self
                .product_activity
                .entry(row.product_id.clone())
                .or_insert_with(|| ProductActivityRow {
                    product_id: row.product_id.clone(),
                    views: 0,
                    carts: 0,
                    checkouts: 0,
                    total: 0,
                });
            match row.event_type.as_str() {
                "product_view" => entry.views = row.event_count,
                "add_to_cart" => entry.carts = row.event_count,
                "checkout" => entry.checkouts = row.event_count,
                _ => {}
            }
            entry.total = entry.views + entry.carts + entry.checkouts;
        }
    }

    fn top_products(&self) -> Vec<&ProductActivityRow> {
        let mut sorted: Vec<_> = self.product_activity.values().collect();
        sorted.sort_by(|a, b| b.total.cmp(&a.total));
        sorted.truncate(8);
        sorted
    }

    fn total_revenue(&self) -> f64 {
        self.order_totals.iter().map(|o| o.gross_revenue).sum()
    }

    fn total_order_count(&self) -> i64 {
        self.order_totals.iter().map(|o| o.order_count).sum()
    }

    fn avg_order_value(&self) -> f64 {
        let count = self.total_order_count();
        if count > 0 {
            self.total_revenue() / count as f64
        } else {
            0.0
        }
    }
}

// ── Dashboard renderer ──────────────────────────────────────────────

fn render_dashboard(state: &DashboardState, uptime: Duration) {
    // Clear screen (ANSI escape)
    print!("\x1B[2J\x1B[1;1H");

    let uptime_secs = uptime.as_secs();
    let mins = uptime_secs / 60;
    let secs = uptime_secs % 60;
    let events_per_sec = if uptime_secs > 0 {
        (state.total_clicks + state.total_orders + state.total_inventory)
            / uptime_secs
    } else {
        0
    };

    println!(
        "+=============================================================================+"
    );
    println!(
        "|          SHOPSTREAM REAL-TIME ANALYTICS DASHBOARD                           |"
    );
    println!(
        "|                    Powered by LaminarDB                                     |"
    );
    println!(
        "+=============================================================================+"
    );
    println!(
        "| Uptime: {:02}:{:02} | Cycle: {:>4} | Throughput: ~{} events/s{:>18}|",
        mins,
        secs,
        state.cycle,
        events_per_sec,
        ""
    );
    println!(
        "+=============================================================================+"
    );

    // KPI Row
    println!(
        "| KEY PERFORMANCE INDICATORS                                                  |"
    );
    println!(
        "+-------------------+-------------------+-------------------+-----------------+"
    );
    println!(
        "| Revenue           | Orders            | Avg Order Value   | Events Pushed   |"
    );
    println!(
        "| ${:>15.2} | {:>17} | ${:>15.2} | {:>15} |",
        state.total_revenue(),
        state.total_order_count(),
        state.avg_order_value(),
        state.total_clicks + state.total_orders + state.total_inventory,
    );
    println!(
        "+-------------------+-------------------+-------------------+-----------------+"
    );

    // Event breakdown
    println!(
        "| Clicks: {:>8}  | Orders: {:>8}  | Inventory: {:>5}  | Sessions: {:>5} |",
        state.total_clicks,
        state.total_orders,
        state.total_inventory,
        state.sessions.len(),
    );
    println!(
        "+=============================================================================+"
    );

    // Trending products
    println!(
        "| PRODUCT ACTIVITY                                                            |"
    );
    println!(
        "+----------------+----------+----------+----------+-----------+---------------+"
    );
    println!(
        "| Product        | Views    | Carts    | Checkout | Total     | Engagement    |"
    );
    println!(
        "+----------------+----------+----------+----------+-----------+---------------+"
    );
    let products = state.top_products();
    if products.is_empty() {
        println!(
            "| (waiting for data...)                                                       |"
        );
    }
    for p in &products {
        let bar_len = (p.total as usize).min(12);
        let bar: String = "#".repeat(bar_len);
        println!(
            "| {:>14} | {:>8} | {:>8} | {:>8} | {:>9} | {:<13} |",
            truncate(&p.product_id, 14),
            p.views,
            p.carts,
            p.checkouts,
            p.total,
            bar,
        );
    }
    println!(
        "+----------------+----------+----------+----------+-----------+---------------+"
    );

    // User sessions
    println!(
        "| USER SESSIONS (Top 10 by activity)                                          |"
    );
    println!(
        "+----------------+--------------+-------------------------------------------+"
    );
    println!(
        "| Session        | User         | Events                                    |"
    );
    println!(
        "+----------------+--------------+-------------------------------------------+"
    );
    if state.sessions.is_empty() {
        println!(
            "| (waiting for data...)                                                       |"
        );
    }
    for s in state.sessions.iter().take(8) {
        let bar_len = (s.event_count as usize).min(40);
        let bar: String = "=".repeat(bar_len);
        println!(
            "| {:>14} | {:>12} | {:>3} {:<38} |",
            truncate(&s.session_id, 14),
            truncate(&s.user_id, 12),
            s.event_count,
            bar,
        );
    }
    println!(
        "+----------------+--------------+-------------------------------------------+"
    );

    // Order totals
    println!(
        "| REVENUE BY USER                                                             |"
    );
    println!(
        "+----------------+----------+------------------+----------------------------+"
    );
    println!(
        "| User           | Orders   | Revenue          | Avg Order Value            |"
    );
    println!(
        "+----------------+----------+------------------+----------------------------+"
    );
    if state.order_totals.is_empty() {
        println!(
            "| (waiting for data...)                                                       |"
        );
    }
    for o in state.order_totals.iter().take(5) {
        println!(
            "| {:>14} | {:>8} | ${:>15.2} | ${:>25.2} |",
            truncate(&o.user_id, 14),
            o.order_count,
            o.gross_revenue,
            o.avg_order_value,
        );
    }
    println!(
        "+----------------+----------+------------------+----------------------------+"
    );

    println!(
        "| Press Ctrl+C to exit                                                        |"
    );
    println!(
        "+=============================================================================+"
    );

    io::stdout().flush().unwrap();
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}..", &s[..max - 2])
    } else {
        format!("{:width$}", s, width = max)
    }
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("SHOPSTREAM_MODE").unwrap_or_else(|_| "embedded".into());

    match mode.as_str() {
        #[cfg(feature = "kafka")]
        "kafka" => run_kafka_mode().await,
        _ => run_embedded_mode().await,
    }
}

/// Embedded mode: in-memory sources, continuous data generation, live dashboard.
async fn run_embedded_mode() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ShopStream: E-Commerce Analytics Demo (Embedded Mode) ===");
    println!();

    // ── Step 1: Build LaminarDB ─────────────────────────────────────
    println!("[1/7] Building LaminarDB with config variables...");
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", "localhost:9092")
        .config_var("GROUP_ID", "shopstream-demo")
        .config_var("ENVIRONMENT", "development")
        .buffer_size(65536)
        .build()
        .await?;
    println!("  Pipeline state: {}", db.pipeline_state());

    // ── Step 2: Create reference tables ─────────────────────────────
    println!("[2/7] Creating reference tables...");
    db.execute(TABLES_SQL).await?;
    verify_table_counts(&db).await;

    // ── Step 3: Create streaming sources ────────────────────────────
    println!("[3/7] Creating streaming sources...");
    db.execute(SOURCES_SQL).await?;
    println!(
        "  Sources: {:?}",
        db.sources().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 4: Create streaming pipelines ──────────────────────────
    println!("[4/7] Creating streaming pipelines...");
    db.execute(STREAMS_SQL).await?;
    println!("  Streams created");

    // ── Step 5: Create sinks ────────────────────────────────────────
    println!("[5/7] Creating sinks...");
    db.execute(SINKS_SQL).await?;
    println!(
        "  Sinks: {:?}",
        db.sinks().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 6: Start pipeline ──────────────────────────────────────
    println!("[6/7] Starting pipeline...");
    db.start().await?;
    println!("  Pipeline state: {}", db.pipeline_state());

    // ── Step 7: Acquire handles and subscribe to output streams ─────
    println!("[7/7] Connecting data generators and subscribing to streams...");

    let sources = SourceHandles {
        clicks: db.source::<ClickEvent>("clickstream")?,
        orders: db.source::<OrderEvent>("orders")?,
        inventory: db.source::<InventoryUpdate>("inventory_updates")?,
    };

    let session_sub =
        db.subscribe::<SessionActivity>("session_activity")?;
    let order_sub = db.subscribe::<OrderTotals>("order_totals")?;
    let product_sub =
        db.subscribe::<ProductActivity>("product_activity")?;

    println!("  Subscribed to: session_activity, order_totals, product_activity");
    println!();
    println!("Pipeline running. Press Ctrl+C to stop.");
    println!();

    // Brief pause so the user can read the startup messages
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Continuous dashboard loop ───────────────────────────────────
    let start_time = Instant::now();
    let mut state = DashboardState::new();

    // Push an initial batch so there's data right away
    let ts = chrono::Utc::now().timestamp_millis();
    sources.push_batch(&mut state, ts, 50, 10, 5);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                state.cycle += 1;

                // Generate and push new events
                let ts = chrono::Utc::now().timestamp_millis();
                sources.push_batch(
                    &mut state,
                    ts,
                    20,  // clicks per cycle
                    5,   // orders per cycle
                    2,   // inventory updates per cycle
                );

                // Advance watermarks
                sources.clicks.watermark(ts + 5_000);
                sources.orders.watermark(ts + 5_000);
                sources.inventory.watermark(ts + 5_000);

                // Drain subscription channels
                drain_subscriptions(
                    &session_sub,
                    &order_sub,
                    &product_sub,
                    &mut state,
                );

                // Render
                render_dashboard(&state, start_time.elapsed());
            }
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────
    // Move cursor below dashboard
    println!();
    println!();
    println!("Shutting down pipeline...");
    db.shutdown().await?;
    println!("  Pipeline state: {}", db.pipeline_state());
    println!();
    println!("=== ShopStream Demo Complete ===");

    Ok(())
}

/// Source handles bundled for convenience.
struct SourceHandles {
    clicks: laminar_db::SourceHandle<ClickEvent>,
    orders: laminar_db::SourceHandle<OrderEvent>,
    inventory: laminar_db::SourceHandle<InventoryUpdate>,
}

impl SourceHandles {
    /// Push a batch of synthetic events into the three sources.
    fn push_batch(
        &self,
        state: &mut DashboardState,
        ts: i64,
        click_count: usize,
        order_count: usize,
        inv_count: usize,
    ) {
        let c = self
            .clicks
            .push_batch(generator::generate_clicks(click_count, ts));
        let o = self
            .orders
            .push_batch(generator::generate_orders(order_count, ts));
        let i = self
            .inventory
            .push_batch(generator::generate_inventory(inv_count, ts));
        state.total_clicks += c as u64;
        state.total_orders += o as u64;
        state.total_inventory += i as u64;
    }
}

/// Poll all subscription channels and merge results into dashboard state.
fn drain_subscriptions(
    session_sub: &laminar_db::TypedSubscription<SessionActivity>,
    order_sub: &laminar_db::TypedSubscription<OrderTotals>,
    product_sub: &laminar_db::TypedSubscription<ProductActivity>,
    state: &mut DashboardState,
) {
    // Drain up to 64 batches per poll cycle
    for _ in 0..64 {
        match session_sub.poll() {
            Some(rows) => state.ingest_sessions(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match order_sub.poll() {
            Some(rows) => state.ingest_order_totals(rows),
            None => break,
        }
    }
    for _ in 0..64 {
        match product_sub.poll() {
            Some(rows) => state.ingest_product_activity(rows),
            None => break,
        }
    }
}

// ── Kafka mode ──────────────────────────────────────────────────────

/// Kafka mode: reads from Kafka topics, writes to Kafka output topics.
#[cfg(feature = "kafka")]
async fn run_kafka_mode() -> Result<(), Box<dyn std::error::Error>> {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::FutureProducer;

    let brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let group_id =
        std::env::var("GROUP_ID").unwrap_or_else(|_| "shopstream-demo".into());
    let gen_rate: usize = std::env::var("GEN_RATE")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap_or(100);

    println!("=== ShopStream: E-Commerce Analytics Demo (Kafka Mode) ===");
    println!();
    println!("  Kafka brokers:  {brokers}");
    println!("  Consumer group: {group_id}");
    println!("  Generator rate: {gen_rate} events/batch");
    println!();

    // ── Step 1: Build LaminarDB ─────────────────────────────────────
    println!("[1/7] Building LaminarDB with Kafka config...");
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", &brokers)
        .config_var("GROUP_ID", &group_id)
        .config_var("ENVIRONMENT", "kafka-demo")
        .buffer_size(65536)
        .build()
        .await?;

    // ── Step 2: Create reference tables ─────────────────────────────
    println!("[2/7] Creating reference tables...");
    db.execute(TABLES_SQL).await?;

    // ── Step 3: Create Kafka sources ────────────────────────────────
    println!("[3/7] Creating Kafka sources (FROM KAFKA)...");
    db.execute(SOURCES_KAFKA_SQL).await?;
    println!(
        "  Sources: {:?}",
        db.sources().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 4: Create streaming pipelines ──────────────────────────
    println!("[4/7] Creating streaming pipelines...");
    db.execute(STREAMS_KAFKA_SQL).await?;

    // ── Step 5: Create Kafka sinks ──────────────────────────────────
    println!("[5/7] Creating Kafka sinks (INTO KAFKA + WHERE)...");
    db.execute(SINKS_KAFKA_SQL).await?;
    println!(
        "  Sinks: {:?}",
        db.sinks().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 6: Start pipeline ──────────────────────────────────────
    println!("[6/7] Starting Kafka pipeline...");
    db.start().await?;
    println!("  Pipeline state: {}", db.pipeline_state());

    // ── Step 7: Generate data and produce to Kafka ──────────────────
    println!("[7/7] Producing synthetic events to Kafka...");
    println!();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let base_ts = chrono::Utc::now().timestamp_millis();

    let clicks = generator::generate_kafka_clicks(gen_rate, base_ts);
    let orders = generator::generate_kafka_orders(gen_rate / 5, base_ts);
    let inventory =
        generator::generate_kafka_inventory(gen_rate / 10, base_ts);

    let click_count =
        generator::produce_to_kafka(&producer, "clickstream", &clicks).await?;
    let order_count =
        generator::produce_to_kafka(&producer, "orders", &orders).await?;
    let inv_count = generator::produce_to_kafka(
        &producer,
        "inventory_updates",
        &inventory,
    )
    .await?;

    println!("  Produced {} clickstream events to Kafka", click_count);
    println!("  Produced {} order events to Kafka", order_count);
    println!("  Produced {} inventory updates to Kafka", inv_count);

    // ── Live dashboard loop ─────────────────────────────────────────
    println!();
    println!("Pipeline running. Press Ctrl+C to stop.");
    println!();

    let start_time = Instant::now();
    let mut cycle = 0u64;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!();
                println!("Ctrl+C received, shutting down...");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                cycle += 1;
                let uptime = start_time.elapsed();
                println!(
                    "=== ShopStream Live [cycle {}] | Uptime: {}s ===",
                    cycle,
                    uptime.as_secs()
                );
                println!(
                    "  Pipeline: {} | Sources: {} | Sinks: {}",
                    db.pipeline_state(),
                    db.source_count(),
                    db.sink_count(),
                );

                // Generate more events periodically
                let ts = chrono::Utc::now().timestamp_millis();
                let batch = generator::generate_kafka_clicks(gen_rate / 10, ts);
                let n = generator::produce_to_kafka(
                    &producer, "clickstream", &batch,
                ).await.unwrap_or(0);
                println!("  Generated {n} more clickstream events");
                println!();
            }
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────
    println!("Shutting down pipeline...");
    db.shutdown().await?;
    println!("  Pipeline state: {}", db.pipeline_state());
    println!();
    println!("=== ShopStream Demo Complete ===");

    Ok(())
}

// ── Helper functions ────────────────────────────────────────────────

async fn verify_table_counts(db: &LaminarDB) {
    let result = db.execute("SELECT COUNT(*) as cnt FROM users").await;
    if let Ok(ExecuteResult::Query(mut q)) = result {
        if let Ok(rows) = q.subscribe::<CountResult>() {
            if let Ok(batch) = rows.recv_timeout(Duration::from_secs(2)) {
                for r in &batch {
                    println!("  users table: {} rows", r.cnt);
                }
            }
        }
    }

    let result = db.execute("SELECT COUNT(*) as cnt FROM products").await;
    if let Ok(ExecuteResult::Query(mut q)) = result {
        if let Ok(rows) = q.subscribe::<CountResult>() {
            if let Ok(batch) = rows.recv_timeout(Duration::from_secs(2)) {
                for r in &batch {
                    println!("  products table: {} rows", r.cnt);
                }
            }
        }
    }
}

// ── Helper types for ad-hoc queries ─────────────────────────────────

#[derive(Debug, laminar_derive::FromRow)]
struct CountResult {
    cnt: i64,
}
