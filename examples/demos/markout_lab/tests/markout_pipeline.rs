use std::collections::BTreeMap;
use std::time::Duration;

use futures::SinkExt;
use laminar_db::{TypedSubscription, TypedSubscriptionFrame};
use markout_lab::config::AppConfig;
use markout_lab::engine::PipelineHarness;
use markout_lab::types::{
    MarketEvent, MarkoutEvent, SimulatedFill, SummaryEvent, FILL_MODEL, HORIZONS_MS, SYMBOL,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

const PLAYER_ID: &str = "integration-player";
const START_MS: i64 = 1_735_689_600_000;
const START_US: i64 = START_MS * 1_000;

struct TickerStub {
    url: String,
    sender: mpsc::Sender<String>,
    task: JoinHandle<()>,
}

impl TickerStub {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut receiver) = mpsc::channel::<String>(32);
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(payload) = receiver.recv().await {
                if websocket.send(Message::Text(payload.into())).await.is_err() {
                    return;
                }
            }
        });
        Self {
            url: format!("ws://{address}"),
            sender,
            task,
        }
    }

    async fn quote(&self, sequence: i64, event_ms: i64, midpoint: f64) {
        let payload = serde_json::json!({
            "e": "24hrTicker",
            "E": event_ms,
            "C": sequence,
            "s": SYMBOL,
            "b": format!("{:.8}", midpoint - 0.5),
            "B": "2.0",
            "a": format!("{:.8}", midpoint + 0.5),
            "A": "2.0"
        });
        self.sender.send(payload.to_string()).await.unwrap();
    }

    async fn stop(self) {
        drop(self.sender);
        self.task.await.unwrap();
    }
}

fn fill(fill_id: &str, symbol: &str, side: &str, fill_px: f64) -> SimulatedFill {
    SimulatedFill {
        demo_run_id: PLAYER_ID.to_string(),
        fill_id: fill_id.to_string(),
        source_event_id: START_MS,
        strategy: "visitor".to_string(),
        symbol: symbol.to_string(),
        side: side.to_string(),
        quantity: 0.01,
        fill_px,
        fee_bps: 0.25,
        mid_at_fill: 100.0,
        spread_bps_at_fill: 100.0,
        decision_reason: "integration test visitor action".to_string(),
        fill_model: FILL_MODEL.to_string(),
        event_ts: START_US,
    }
}

async fn collect_until(
    subscription: &mut TypedSubscription<MarkoutEvent>,
    rows: &mut Vec<MarkoutEvent>,
    expected: usize,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while rows.len() < expected {
            match subscription.next_frame().await.unwrap() {
                Some(TypedSubscriptionFrame::Rows { rows: emitted, .. }) => {
                    rows.extend(emitted);
                }
                Some(TypedSubscriptionFrame::Barrier { .. }) => {}
                None => panic!("markout subscription closed"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {expected} markout rows, received {}", rows.len()));
}

async fn collect_market(subscription: &mut TypedSubscription<MarketEvent>) -> MarketEvent {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match subscription.next_frame().await.unwrap() {
                Some(TypedSubscriptionFrame::Rows { rows, .. }) => {
                    if let Some(row) = rows.into_iter().next() {
                        return row;
                    }
                }
                Some(TypedSubscriptionFrame::Barrier { .. }) => {}
                None => panic!("market subscription closed"),
            }
        }
    })
    .await
    .expect("live market row within the deadline")
}

async fn collect_summary_until(
    subscription: &mut TypedSubscription<SummaryEvent>,
    predicate: impl Fn(&SummaryEvent) -> bool,
) -> SummaryEvent {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match subscription.next_frame().await.unwrap() {
                Some(TypedSubscriptionFrame::Rows { rows, .. }) => {
                    if let Some(row) = rows.into_iter().rev().find(&predicate) {
                        return row;
                    }
                }
                Some(TypedSubscriptionFrame::Barrier { .. }) => {}
                None => panic!("summary subscription closed"),
            }
        }
    })
    .await
    .expect("matching dashboard summary within the deadline")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_pipeline_emits_progressive_live_feed_markouts() {
    let ticker = TickerStub::start().await;
    let harness =
        PipelineHarness::start(&ticker.url, Duration::from_secs(5), Duration::from_secs(5))
            .await
            .unwrap();
    let inputs = harness.inputs();
    let mut market_subscription = harness.market_subscription().await.unwrap();
    let mut subscription = harness.markout_subscription().await.unwrap();
    let mut summary_subscription = harness.summary_subscription().await.unwrap();
    let mut sequence = START_MS;
    let mut rows = Vec::new();

    ticker.quote(sequence, START_MS, 100.0).await;
    sequence += 1;
    let market = collect_market(&mut market_subscription).await;
    assert_eq!(market.symbol, SYMBOL);
    assert_eq!(market.mid_px, 100.0);

    inputs
        .push_fills(&[
            fill("buy-up", SYMBOL, "BUY", 99.5),
            fill("sell-up", SYMBOL, "SELL", 100.5),
            fill("missing", "MISSING", "BUY", 99.5),
        ])
        .await
        .unwrap();
    ticker.quote(sequence, START_MS + 250, 100.0).await;
    sequence += 1;
    collect_until(&mut subscription, &mut rows, 3).await;
    assert!(rows.iter().all(|row| row.horizon_ms == 0));

    let summary_0s = collect_summary_until(&mut summary_subscription, |row| {
        row.strategy == "visitor" && row.spread_capture_0s_bps.is_some()
    })
    .await;
    assert!(summary_0s.weighted_markout_5s_bps.is_none());
    assert!(summary_0s.hypothetical_pnl_30s.is_none());

    for (horizon_ms, midpoint) in [
        (1_000, 102.0),
        (5_000, 103.0),
        (15_000, 101.0),
        (30_000, 104.0),
    ] {
        ticker
            .quote(sequence, START_MS + horizon_ms, midpoint)
            .await;
        sequence += 1;
        ticker
            .quote(sequence, START_MS + horizon_ms + 250, midpoint)
            .await;
        sequence += 1;
        let reached = HORIZONS_MS
            .iter()
            .filter(|&&horizon| horizon <= horizon_ms)
            .count();
        collect_until(&mut subscription, &mut rows, reached * 3).await;
    }

    assert_eq!(rows.len(), HORIZONS_MS.len() * 3);
    let by_key = rows
        .iter()
        .map(|row| ((row.fill_id.as_str(), row.horizon_ms), row))
        .collect::<BTreeMap<_, _>>();
    assert!(by_key[&("buy-up", 1_000)].gross_markout_bps.unwrap() > 0.0);
    assert!(by_key[&("sell-up", 1_000)].gross_markout_bps.unwrap() < 0.0);

    for row in rows.iter().filter(|row| row.covered) {
        let decomposed = row.spread_capture_bps + row.post_fill_drift_bps.unwrap();
        let gross = row.gross_markout_bps.unwrap();
        assert!((gross - decomposed).abs() < 1e-9);
    }
    let missing = rows
        .iter()
        .filter(|row| row.fill_id == "missing")
        .collect::<Vec<_>>();
    assert_eq!(missing.len(), HORIZONS_MS.len());
    assert!(missing.iter().all(|row| {
        !row.covered
            && row.reference_quote_ts.is_none()
            && row.future_mid_px.is_none()
            && row.gross_markout_bps.is_none()
            && row.net_markout_pnl.is_none()
    }));

    harness.shutdown().await.unwrap();
    ticker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn demo_terminates_when_the_required_feed_disconnects() {
    let ticker = TickerStub::start().await;
    let config = AppConfig {
        host: "127.0.0.1".parse().unwrap(),
        port: 0,
        feed_url: ticker.url.clone(),
        feed_start_timeout_secs: 5,
        feed_stale_after_secs: 2,
        maker_fee_bps: 0.0,
    };
    let app = tokio::spawn(markout_lab::run(config));

    ticker.quote(START_MS, START_MS, 100.0).await;
    ticker.stop().await;

    let result = tokio::time::timeout(Duration::from_secs(10), app)
        .await
        .expect("demo must terminate after its required feed disconnects")
        .expect("demo task must not panic");
    let error = result.expect_err("a lost exchange feed must be fatal");
    assert!(
        format!("{error:#}").contains("required live market feed stopped"),
        "unexpected shutdown error: {error:#}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn demo_terminates_when_the_endpoint_sends_no_market_data() {
    let ticker = TickerStub::start().await;
    let config = AppConfig {
        host: "127.0.0.1".parse().unwrap(),
        port: 0,
        feed_url: ticker.url.clone(),
        feed_start_timeout_secs: 5,
        feed_stale_after_secs: 2,
        maker_fee_bps: 0.0,
    };

    let result = tokio::time::timeout(Duration::from_secs(8), markout_lab::run(config))
        .await
        .expect("demo must terminate when no market messages arrive");
    ticker.stop().await;

    let error = result.expect_err("an exchange endpoint without market data must be fatal");
    assert!(
        format!("{error:#}").contains("required live market feed stopped"),
        "unexpected shutdown error: {error:#}"
    );
}
