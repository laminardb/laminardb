//! Simulated broker drop copy for the FIX desk demo in `examples/fix-desk`.
//!
//! A QuickFIX acceptor streams ExecutionReports for a multi-account equity order flow:
//! acknowledgements, partial and full fills, amendments, cancels, and rejects, with algorithmic
//! bursts and occasional oversized orders. Reports keep flowing while the client is down; the
//! engine stores them and resends them when the client logs on again. Every report is appended to
//! `<data>/ledger.csv`, the venue's side of a drop-copy reconciliation.
//!
//! SYNTHETIC: prices, accounts, and order flow are simulated; this is not a venue profile.
//!
//! ```text
//! cargo run --release -p laminar-connectors --no-default-features --features fix,testing \
//!     --example fix_venue -- \
//!     --dictionary FIX44.xml [--data data/venue] [--port 9878] [--rate 20] \
//!     [--sender BROKER] [--target DESK] [--begin-string FIX.4.4] [--field TAG=VALUE|VALUE ...]
//! ```
//!
//! `--field` adds a venue-specific field to every report with one of the values chosen at random;
//! the client's dictionary must define it.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::Utc;
use laminar_connectors::fix::testing::{FixTestVenue, TestMessage};
use rand::rngs::ThreadRng;
use rand::RngExt;

const STEP: Duration = Duration::from_millis(20);
const STATUS_EVERY: Duration = Duration::from_secs(5);
const MAX_LIVE_ORDERS: usize = 400;
const MAX_ORDER_QTY: u32 = 25_000;
const COMMISSION_PER_SHARE: f64 = 0.0035;
/// Illustrative reference prices; the simulation walks them randomly.
const UNIVERSE: &[(&str, f64)] = &[
    ("AAPL", 228.40),
    ("MSFT", 417.10),
    ("NVDA", 131.20),
    ("AMZN", 186.70),
    ("GOOGL", 165.30),
    ("META", 571.80),
    ("TSLA", 251.30),
    ("JPM", 211.50),
];
const ACCOUNTS: &[&str] = &["EQ-MM", "EQ-ARB", "EQ-MOMENTUM", "CLIENT-FLOW"];
const MARKETS: &[&str] = &["XNAS", "XNYS", "ARCX", "BATS", "EDGX", "IEXG"];
/// OrdRejReason (103) and Text (58) of risk-check rejects.
const REJECTS: &[(&str, &str)] = &[
    ("0", "Broker credit limit exceeded"),
    ("99", "Price outside collar"),
    ("6", "Duplicate order"),
];

struct Options {
    port: u16,
    dictionary: PathBuf,
    data: PathBuf,
    begin_string: String,
    sender: String,
    target: String,
    rate: f64,
    fields: Vec<(i32, Vec<String>)>,
}

fn parse_options() -> Result<Options, String> {
    let mut options = Options {
        port: 9878,
        dictionary: PathBuf::new(),
        data: PathBuf::from("data/venue"),
        begin_string: "FIX.4.4".into(),
        sender: "BROKER".into(),
        target: "DESK".into(),
        rate: 20.0,
        fields: Vec::new(),
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--port" => options.port = value.parse().map_err(|_| "--port must be a port")?,
            "--dictionary" => options.dictionary = value.into(),
            "--data" => options.data = value.into(),
            "--begin-string" => options.begin_string = value,
            "--sender" => options.sender = value,
            "--target" => options.target = value,
            "--rate" => {
                options.rate = value
                    .parse()
                    .ok()
                    .filter(|rate| (0.1..=5000.0).contains(rate))
                    .ok_or("--rate must be 0.1 to 5000 orders per second")?;
            }
            "--field" => options.fields.push(parse_field(&value)?),
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    if options.dictionary.as_os_str().is_empty() {
        return Err("--dictionary is required".into());
    }
    Ok(options)
}

fn parse_field(spec: &str) -> Result<(i32, Vec<String>), String> {
    let invalid = || format!("--field {spec}: expected TAG=VALUE or TAG=VALUE|VALUE");
    let (tag, values) = spec.split_once('=').ok_or_else(invalid)?;
    let tag = tag
        .parse()
        .ok()
        .filter(|tag| *tag > 0)
        .ok_or_else(invalid)?;
    let values: Vec<String> = values.split('|').map(str::to_string).collect();
    if values.iter().any(String::is_empty) {
        return Err(invalid());
    }
    Ok((tag, values))
}

struct Order {
    order_id: String,
    cl_ord_id: String,
    account: &'static str,
    instrument: usize,
    buy: bool,
    qty: u32,
    price: f64,
    cum_qty: u32,
    filled_notional: f64,
    marketable: bool,
    since: Instant,
}

impl Order {
    fn leaves(&self) -> u32 {
        self.qty - self.cum_qty
    }
}

struct Fill {
    qty: u32,
    px: f64,
    market: &'static str,
    liquidity: &'static str,
}

#[derive(Clone, Copy)]
struct Burst {
    account: &'static str,
    instrument: usize,
    buy: bool,
    remaining: u32,
}

struct Report {
    message: TestMessage,
    ledger: String,
    exec_type: &'static str,
}

struct Desk {
    rng: ThreadRng,
    prices: Vec<f64>,
    orders: Vec<Order>,
    burst: Option<Burst>,
    run: String,
    next_id: u64,
    rate: f64,
    fields: Vec<(i32, Vec<String>)>,
}

impl Desk {
    fn new(rate: f64, fields: Vec<(i32, Vec<String>)>) -> Self {
        Self {
            rng: rand::rng(),
            prices: UNIVERSE.iter().map(|(_, price)| *price).collect(),
            orders: Vec::new(),
            burst: None,
            // Keeps identifiers unique across venue restarts.
            run: Utc::now().format("%m%d%H%M%S").to_string(),
            next_id: 0,
            rate,
            fields,
        }
    }

    fn step(&mut self) -> Vec<Report> {
        let mut reports = Vec::new();
        for price in &mut self.prices {
            *price *= 1.0 + 0.00005 * gaussian(&mut self.rng);
        }
        self.arrivals(&mut reports);
        self.work_orders(&mut reports);
        reports
    }

    fn arrivals(&mut self, reports: &mut Vec<Report>) {
        let mut expected = self.rate * STEP.as_secs_f64();
        while expected > 0.0 {
            if self.rng.random::<f64>() < expected.min(1.0) {
                let account = ACCOUNTS[self.rng.random_range(0..ACCOUNTS.len())];
                let instrument = self.rng.random_range(0..UNIVERSE.len());
                let buy = self.rng.random_bool(0.5);
                let marketable = self.rng.random_bool(0.3);
                reports.push(self.new_order(account, instrument, buy, marketable));
            }
            expected -= 1.0;
        }
        // An algorithm slices a parent order into marketable children, one per step.
        if self.burst.is_none() && self.rng.random_bool(0.0004) {
            self.burst = Some(Burst {
                account: "EQ-MOMENTUM",
                instrument: self.rng.random_range(0..UNIVERSE.len()),
                buy: self.rng.random_bool(0.5),
                remaining: 25,
            });
        }
        if let Some(burst) = self.burst {
            self.burst = (burst.remaining > 1).then_some(Burst {
                remaining: burst.remaining - 1,
                ..burst
            });
            reports.push(self.new_order(burst.account, burst.instrument, burst.buy, true));
        }
    }

    fn new_order(
        &mut self,
        account: &'static str,
        instrument: usize,
        buy: bool,
        marketable: bool,
    ) -> Report {
        let id = self.next_id();
        let mut qty: u32 = 100 * self.rng.random_range(1..=20);
        if self.rng.random_bool(0.004) {
            qty *= self.rng.random_range(10..=30);
        }
        let offset = if marketable {
            0.0005
        } else {
            -self.rng.random_range(0.0001..0.0015)
        };
        let mid = self.prices[instrument];
        let order = Order {
            order_id: format!("{}-{id}", self.run),
            cl_ord_id: format!("{account}-{}-{id}", self.run),
            account,
            instrument,
            buy,
            qty,
            price: cents(mid * if buy { 1.0 + offset } else { 1.0 - offset }),
            cum_qty: 0,
            filled_notional: 0.0,
            marketable,
            since: Instant::now(),
        };
        let reject = if qty > MAX_ORDER_QTY {
            Some(("3", "Order exceeds max order size"))
        } else if self.rng.random_bool(0.015) {
            Some(REJECTS[self.rng.random_range(0..REJECTS.len())])
        } else {
            None
        };
        let exec_id = self.exec_id();
        let mut fields = self.venue_fields();
        let Some((reason, text)) = reject else {
            let report = execution_report(&order, exec_id, "0", None, fields);
            self.orders.push(order);
            return report;
        };
        fields.extend([(103, reason.to_string()), (58, text.to_string())]);
        execution_report(&order, exec_id, "8", None, fields)
    }

    fn work_orders(&mut self, reports: &mut Vec<Report>) {
        let mut index = 0;
        while index < self.orders.len() {
            let order = &self.orders[index];
            let fill_chance = if order.marketable { 0.5 } else { 0.015 };
            let resting = !order.marketable && order.since.elapsed() > Duration::from_secs(3);
            let (report, done) = if self.rng.random_bool(fill_chance) {
                self.fill(index)
            } else if resting && self.rng.random_bool(0.003) {
                (self.cancel(index), true)
            } else if resting && self.rng.random_bool(0.002) {
                (self.replace(index), false)
            } else {
                index += 1;
                continue;
            };
            reports.push(report);
            if done {
                self.orders.swap_remove(index);
            } else {
                index += 1;
            }
        }
        while self.orders.len() > MAX_LIVE_ORDERS {
            reports.push(self.cancel(0));
            self.orders.swap_remove(0);
        }
    }

    fn fill(&mut self, index: usize) -> (Report, bool) {
        let exec_id = self.exec_id();
        let fields = self.venue_fields();
        let market = MARKETS[self.rng.random_range(0..MARKETS.len())];
        let lots = self.rng.random_range(1..=self.orders[index].leaves() / 100);
        let mid = self.prices[self.orders[index].instrument];
        let order = &mut self.orders[index];
        // Resting orders fill at their limit; marketable ones take the market up to it.
        let (px, liquidity) = match (order.marketable, order.buy) {
            (false, _) => (order.price, "1"),
            (true, true) => (cents(mid).min(order.price), "2"),
            (true, false) => (cents(mid).max(order.price), "2"),
        };
        let fill = Fill {
            qty: 100 * lots,
            px,
            market,
            liquidity,
        };
        order.cum_qty += fill.qty;
        order.filled_notional += f64::from(fill.qty) * px;
        let done = order.leaves() == 0;
        (
            execution_report(order, exec_id, "F", Some(fill), fields),
            done,
        )
    }

    fn cancel(&mut self, index: usize) -> Report {
        let exec_id = self.exec_id();
        let fields = self.venue_fields();
        execution_report(&self.orders[index], exec_id, "4", None, fields)
    }

    fn replace(&mut self, index: usize) -> Report {
        let exec_id = self.exec_id();
        let mut fields = self.venue_fields();
        let id = self.next_id();
        let ticks = f64::from(self.rng.random_range(1..=5_u8));
        let order = &mut self.orders[index];
        let cl_ord_id = format!("{}-{}-{id}", order.account, self.run);
        fields.push((41, std::mem::replace(&mut order.cl_ord_id, cl_ord_id)));
        // Amend toward the market.
        order.price = cents(order.price + if order.buy { 0.01 } else { -0.01 } * ticks);
        execution_report(order, exec_id, "5", None, fields)
    }

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn exec_id(&mut self) -> String {
        let id = self.next_id();
        format!("{}-X{id}", self.run)
    }

    fn venue_fields(&mut self) -> Vec<(i32, String)> {
        self.fields
            .iter()
            .map(|(tag, values)| (*tag, values[self.rng.random_range(0..values.len())].clone()))
            .collect()
    }
}

fn execution_report(
    order: &Order,
    exec_id: String,
    exec_type: &'static str,
    fill: Option<Fill>,
    extra: Vec<(i32, String)>,
) -> Report {
    let complete = order.cum_qty == order.qty;
    let ord_status = match exec_type {
        "4" | "8" => exec_type,
        _ if complete => "2",
        _ if order.cum_qty > 0 => "1",
        _ => "0",
    };
    let leaves = if complete || matches!(exec_type, "4" | "8") {
        0
    } else {
        order.leaves()
    };
    let avg_px = if order.cum_qty == 0 {
        0.0
    } else {
        order.filled_notional / f64::from(order.cum_qty)
    };
    let symbol = UNIVERSE[order.instrument].0;
    let side = if order.buy { "1" } else { "2" };
    let transact_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();
    let mut fields = vec![
        (37, order.order_id.clone()),
        (11, order.cl_ord_id.clone()),
        (17, exec_id.clone()),
        (150, exec_type.to_string()),
        (39, ord_status.to_string()),
        (1, order.account.to_string()),
        (55, symbol.to_string()),
        (54, side.to_string()),
        (38, order.qty.to_string()),
        (40, "2".to_string()),
        (44, format!("{:.2}", order.price)),
        (59, "0".to_string()),
        (151, leaves.to_string()),
        (14, order.cum_qty.to_string()),
        (6, format!("{avg_px:.4}")),
        (15, "USD".to_string()),
        (60, transact_time.clone()),
    ];
    let (last_qty, last_px) = match &fill {
        Some(fill) => {
            let commission = f64::from(fill.qty) * COMMISSION_PER_SHARE;
            fields.extend([
                (32, fill.qty.to_string()),
                (31, format!("{:.2}", fill.px)),
                (30, fill.market.to_string()),
                (851, fill.liquidity.to_string()),
                (12, format!("{commission:.2}")),
                (13, "3".to_string()),
            ]);
            (fill.qty.to_string(), format!("{:.2}", fill.px))
        }
        None => (String::new(), String::new()),
    };
    fields.extend(extra);
    Report {
        message: TestMessage {
            msg_type: "8".into(),
            fields,
            ..TestMessage::default()
        },
        ledger: format!(
            "{exec_id},{exec_type},{},{symbol},{side},{last_qty},{last_px},{transact_time}\n",
            order.account
        ),
        exec_type,
    }
}

fn cents(price: f64) -> f64 {
    (price * 100.0).round() / 100.0
}

fn gaussian(rng: &mut ThreadRng) -> f64 {
    let uniform: f64 = rng.random_range(f64::EPSILON..1.0);
    let angle: f64 = rng.random::<f64>() * std::f64::consts::TAU;
    (-2.0 * uniform.ln()).sqrt() * angle.cos()
}

#[derive(Default)]
struct Totals {
    reports: u64,
    fills: u64,
    rejects: u64,
}

async fn run(venue: &FixTestVenue, options: Options) -> Result<(), Box<dyn Error>> {
    let mut shutdown = std::pin::pin!(tokio::signal::ctrl_c());
    println!(
        "{} listening on port {}; waiting for {} to log on",
        options.sender, options.port, options.target
    );
    while !venue.logged_on() {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    let path = options.data.join("ledger.csv");
    let file = OpenOptions::new().create(true).append(true).open(&path)?;
    let fresh = file.metadata()?.len() == 0;
    let mut ledger = BufWriter::new(file);
    if fresh {
        ledger
            .write_all(b"exec_id,exec_type,account,symbol,side,last_qty,last_px,transact_time\n")?;
    }
    println!("streaming; ledger {}", path.display());
    let mut desk = Desk::new(options.rate, options.fields);
    let mut totals = Totals::default();
    let mut ticker = tokio::time::interval(STEP);
    let mut status = Instant::now();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {}
        }
        for report in desk.step() {
            venue.send(&report.message)?;
            ledger.write_all(report.ledger.as_bytes())?;
            totals.reports += 1;
            totals.fills += u64::from(report.exec_type == "F");
            totals.rejects += u64::from(report.exec_type == "8");
        }
        // Between steps the ledger matches what was sent, even if the venue is killed.
        ledger.flush()?;
        if status.elapsed() >= STATUS_EVERY {
            status = Instant::now();
            println!(
                "{} reports ({} fills, {} rejects), {} live orders, next seq {}, client {}, \
                 {} session rejects from the client",
                totals.reports,
                totals.fills,
                totals.rejects,
                desk.orders.len(),
                venue.next_sender()?,
                if venue.logged_on() {
                    "logged on"
                } else {
                    "DOWN (queuing for resend)"
                },
                venue.rejects().len(),
            );
        }
    }
    ledger.flush()?;
    println!(
        "stopped after {} reports; ledger {}",
        totals.reports,
        path.display()
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = parse_options()?;
    std::fs::create_dir_all(&options.data)?;
    let venue = FixTestVenue::listen(
        options.port,
        &options.data.join("store"),
        &options.dictionary,
        &options.begin_string,
        &options.sender,
        &options.target,
        &[],
    )?;
    let result = run(&venue, options).await;
    venue.stop()?;
    result
}
