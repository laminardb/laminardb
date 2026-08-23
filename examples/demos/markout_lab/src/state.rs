use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use laminar_db::{FromBatch, TypedSubscription, TypedSubscriptionFrame};
use serde::Serialize;
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::engine::PipelineHarness;
use crate::types::{CurveEvent, FillEvent, MarketEvent, MarkoutEvent, SummaryEvent};

const EVENT_CAPACITY: usize = 2_048;
const RECENT_FILL_LIMIT: usize = 256;
const FEED_SOURCE: &str = "Binance BTCUSDT public WebSocket";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedPhase {
    Connecting,
    Live,
    Unavailable,
    Faulted,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeedStatus {
    pub phase: FeedPhase,
    pub source: &'static str,
    pub message: String,
    pub last_event_ts: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct UiEvent {
    pub name: &'static str,
    pub data: String,
    player_id: Option<String>,
}

impl UiEvent {
    fn global_json(name: &'static str, value: &impl Serialize) -> Result<Self> {
        Ok(Self {
            name,
            data: serde_json::to_string(value).context("serialize dashboard event")?,
            player_id: None,
        })
    }

    fn player_json(name: &'static str, player_id: &str, value: &impl Serialize) -> Result<Self> {
        Ok(Self {
            name,
            data: serde_json::to_string(value).context("serialize player dashboard event")?,
            player_id: Some(player_id.to_string()),
        })
    }

    #[must_use]
    pub fn visible_to(&self, player_id: &str) -> bool {
        self.player_id
            .as_deref()
            .is_none_or(|scope| scope == player_id)
    }

    #[must_use]
    pub fn global(name: &'static str, data: String) -> Self {
        Self {
            name,
            data,
            player_id: None,
        }
    }
}

struct DashboardState {
    status: FeedStatus,
    market: Option<MarketEvent>,
    market_received_at: Option<Instant>,
    fills: VecDeque<FillEvent>,
    markouts: BTreeMap<(String, String, i64), MarkoutEvent>,
    curves: BTreeMap<(String, String, i64), CurveEvent>,
    summaries: BTreeMap<(String, String), SummaryEvent>,
}

impl DashboardState {
    fn new() -> Self {
        Self {
            status: FeedStatus {
                phase: FeedPhase::Connecting,
                source: FEED_SOURCE,
                message: "Connecting to the live Binance market feed".to_string(),
                last_event_ts: None,
            },
            market: None,
            market_received_at: None,
            fills: VecDeque::with_capacity(RECENT_FILL_LIMIT),
            markouts: BTreeMap::new(),
            curves: BTreeMap::new(),
            summaries: BTreeMap::new(),
        }
    }

    fn has_player(&self, player_id: &str) -> bool {
        self.fills.iter().any(|fill| fill.demo_run_id == player_id)
    }

    fn evict_oldest_fill(&mut self) {
        let Some(evicted) = self.fills.pop_front() else {
            return;
        };
        self.markouts.retain(|(player_id, fill_id, _), _| {
            player_id != &evicted.demo_run_id || fill_id != &evicted.fill_id
        });
        if !self.has_player(&evicted.demo_run_id) {
            self.curves
                .retain(|(player_id, _, _), _| player_id != &evicted.demo_run_id);
            self.summaries
                .retain(|(player_id, _), _| player_id != &evicted.demo_run_id);
        }
    }
}

#[derive(Clone)]
pub struct EventHub {
    state: std::sync::Arc<RwLock<DashboardState>>,
    sender: broadcast::Sender<UiEvent>,
}

impl EventHub {
    #[must_use]
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EVENT_CAPACITY);
        Self {
            state: std::sync::Arc::new(RwLock::new(DashboardState::new())),
            sender,
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<UiEvent> {
        self.sender.subscribe()
    }

    pub async fn status(&self) -> FeedStatus {
        self.state.read().await.status.clone()
    }

    pub async fn latest_market(&self, max_age: Duration) -> Option<MarketEvent> {
        let state = self.state.read().await;
        let received_at = state.market_received_at?;
        (state.status.phase == FeedPhase::Live && received_at.elapsed() <= max_age)
            .then(|| state.market.clone())
            .flatten()
    }

    pub async fn fault(&self, message: impl Into<String>) {
        let status = {
            let mut state = self.state.write().await;
            state.status.phase = FeedPhase::Faulted;
            state.status.message = message.into();
            state.status.clone()
        };
        if let Ok(event) = UiEvent::global_json("status", &status) {
            self.emit(event);
        }
    }

    pub async fn apply_market(&self, row: MarketEvent) -> Result<()> {
        let market_event = UiEvent::global_json("market", &row)?;
        let status = {
            let mut state = self.state.write().await;
            let transitioned = state.status.phase != FeedPhase::Live;
            state.market = Some(row.clone());
            state.market_received_at = Some(Instant::now());
            state.status.phase = FeedPhase::Live;
            state.status.message = "Live Binance market feed connected".to_string();
            state.status.last_event_ts = Some(row.event_ts);
            transitioned.then(|| state.status.clone())
        };
        self.emit(market_event);
        if let Some(status) = status {
            self.emit(UiEvent::global_json("status", &status)?);
        }
        Ok(())
    }

    pub async fn mark_unavailable_if_stale(
        &self,
        started_at: Instant,
        startup_timeout: Duration,
        stale_after: Duration,
    ) -> Result<()> {
        let updated = {
            let mut state = self.state.write().await;
            let unavailable = match state.market_received_at {
                Some(received_at) => received_at.elapsed() > stale_after,
                None => started_at.elapsed() > startup_timeout,
            };
            if !unavailable
                || matches!(
                    state.status.phase,
                    FeedPhase::Unavailable | FeedPhase::Faulted
                )
            {
                None
            } else {
                state.status.phase = FeedPhase::Unavailable;
                state.status.message = if state.market_received_at.is_some() {
                    "Live market feed stopped; simulated orders are disabled".to_string()
                } else {
                    "Live market feed unavailable; no fallback data is running".to_string()
                };
                Some(state.status.clone())
            }
        };
        if let Some(status) = updated {
            self.emit(UiEvent::global_json("status", &status)?);
        }
        Ok(())
    }

    pub async fn apply_fill(&self, row: FillEvent) -> Result<()> {
        let event = UiEvent::player_json("fill", &row.demo_run_id, &row)?;
        let accepted = {
            let mut state = self.state.write().await;
            if state.fills.iter().any(|fill| fill.fill_id == row.fill_id) {
                false
            } else {
                if state.fills.len() == RECENT_FILL_LIMIT {
                    state.evict_oldest_fill();
                }
                state.fills.push_back(row);
                true
            }
        };
        if accepted {
            self.emit(event);
        }
        Ok(())
    }

    pub async fn apply_markout(&self, row: MarkoutEvent) -> Result<()> {
        let event = UiEvent::player_json("markout", &row.demo_run_id, &row)?;
        let accepted = {
            let mut state = self.state.write().await;
            if !state.has_player(&row.demo_run_id) {
                false
            } else {
                state
                    .markouts
                    .insert(
                        (row.demo_run_id.clone(), row.fill_id.clone(), row.horizon_ms),
                        row,
                    )
                    .is_none()
            }
        };
        if accepted {
            self.emit(event);
        }
        Ok(())
    }

    pub async fn apply_curve(&self, row: CurveEvent) -> Result<()> {
        let event = UiEvent::player_json("curve", &row.demo_run_id, &row)?;
        let accepted = {
            let mut state = self.state.write().await;
            if !state.has_player(&row.demo_run_id) {
                false
            } else {
                let key = (
                    row.demo_run_id.clone(),
                    row.strategy.clone(),
                    row.horizon_ms,
                );
                if state.curves.get(&key) == Some(&row) {
                    false
                } else {
                    state.curves.insert(key, row);
                    true
                }
            }
        };
        if accepted {
            self.emit(event);
        }
        Ok(())
    }

    pub async fn apply_summary(&self, row: SummaryEvent) -> Result<()> {
        let merged = {
            let mut state = self.state.write().await;
            if !state.has_player(&row.demo_run_id) {
                None
            } else {
                let key = (row.demo_run_id.clone(), row.strategy.clone());
                let merged = if let Some(current) = state.summaries.get_mut(&key) {
                    let previous = current.clone();
                    merge_summary(current, row);
                    (*current != previous).then(|| current.clone())
                } else {
                    state.summaries.insert(key, row.clone());
                    Some(row)
                };
                merged
            }
        };
        if let Some(merged) = merged {
            self.emit(UiEvent::player_json(
                "summary",
                &merged.demo_run_id,
                &merged,
            )?);
        }
        Ok(())
    }

    pub async fn snapshot_events(&self, player_id: &str) -> Result<Vec<UiEvent>> {
        let state = self.state.read().await;
        let mut events = vec![UiEvent::global_json("status", &state.status)?];
        if let Some(market) = &state.market {
            events.push(UiEvent::global_json("market", market)?);
        }
        let visible_fill_ids = state
            .fills
            .iter()
            .filter(|fill| fill.demo_run_id == player_id)
            .map(|fill| fill.fill_id.as_str())
            .collect::<BTreeSet<_>>();
        for fill in state
            .fills
            .iter()
            .filter(|fill| fill.demo_run_id == player_id)
        {
            events.push(UiEvent::player_json("fill", player_id, fill)?);
        }
        for ((scope, fill_id, _), markout) in &state.markouts {
            if scope == player_id && visible_fill_ids.contains(fill_id.as_str()) {
                events.push(UiEvent::player_json("markout", player_id, markout)?);
            }
        }
        for ((scope, _, _), curve) in &state.curves {
            if scope == player_id {
                events.push(UiEvent::player_json("curve", player_id, curve)?);
            }
        }
        for ((scope, _), summary) in &state.summaries {
            if scope == player_id {
                events.push(UiEvent::player_json("summary", player_id, summary)?);
            }
        }
        Ok(events)
    }

    fn emit(&self, event: UiEvent) {
        let _ = self.sender.send(event);
    }
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

fn merge_summary(current: &mut SummaryEvent, update: SummaryEvent) {
    current.horizon_ms = current.horizon_ms.max(update.horizon_ms);
    if update.spread_capture_0s_bps.is_some() {
        current.spread_capture_0s_bps = update.spread_capture_0s_bps;
    }
    if update.weighted_markout_5s_bps.is_some() {
        current.weighted_markout_5s_bps = update.weighted_markout_5s_bps;
    }
    if update.hypothetical_pnl_30s.is_some() {
        current.hypothetical_pnl_30s = update.hypothetical_pnl_30s;
    }
    if update.filled_notional.is_some() {
        current.filled_notional = update.filled_notional;
    }
    if update.adverse_fill_rate_5s.is_some() {
        current.adverse_fill_rate_5s = update.adverse_fill_rate_5s;
    }
}

pub struct FeedWatchdog {
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl FeedWatchdog {
    pub async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

#[must_use]
pub fn start_feed_watchdog(
    hub: EventHub,
    startup_timeout: Duration,
    stale_after: Duration,
) -> FeedWatchdog {
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let started_at = Instant::now();
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    if let Err(error) = hub
                        .mark_unavailable_if_stale(started_at, startup_timeout, stale_after)
                        .await
                    {
                        hub.fault(format!("Feed watchdog failed: {error:#}")).await;
                        return;
                    }
                }
                _ = &mut stopped => return,
            }
        }
    });
    FeedWatchdog { stop, task }
}

pub struct SubscriptionTasks {
    tasks: Vec<JoinHandle<()>>,
}

impl SubscriptionTasks {
    pub async fn stop(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

pub async fn start_subscription_tasks(
    pipeline: &PipelineHarness,
    hub: EventHub,
) -> Result<SubscriptionTasks> {
    let market = pipeline.market_subscription().await?;
    let fills = pipeline.fill_subscription().await?;
    let markouts = pipeline.markout_subscription().await?;
    let curves = pipeline.curve_subscription().await?;
    let summaries = pipeline.summary_subscription().await?;
    let tasks = vec![
        spawn_subscription("market", market, hub.clone(), |hub, row| async move {
            hub.apply_market(row).await
        }),
        spawn_subscription("fill", fills, hub.clone(), |hub, row| async move {
            hub.apply_fill(row).await
        }),
        spawn_subscription("markout", markouts, hub.clone(), |hub, row| async move {
            hub.apply_markout(row).await
        }),
        spawn_subscription("curve", curves, hub.clone(), |hub, row| async move {
            hub.apply_curve(row).await
        }),
        spawn_subscription("summary", summaries, hub, |hub, row| async move {
            hub.apply_summary(row).await
        }),
    ];
    Ok(SubscriptionTasks { tasks })
}

fn spawn_subscription<T, Apply, Applied>(
    name: &'static str,
    subscription: TypedSubscription<T>,
    hub: EventHub,
    apply: Apply,
) -> JoinHandle<()>
where
    T: FromBatch + Send + 'static,
    Apply: Fn(EventHub, T) -> Applied + Send + Sync + 'static,
    Applied: Future<Output = Result<()>> + Send,
{
    tokio::spawn(async move {
        let fault_hub = hub.clone();
        if let Err(error) = pump_subscription(subscription, hub, apply).await {
            fault_hub
                .fault(format!("{name} subscription failed: {error:#}"))
                .await;
        }
    })
}

async fn pump_subscription<T, Apply, Applied>(
    mut subscription: TypedSubscription<T>,
    hub: EventHub,
    apply: Apply,
) -> Result<()>
where
    T: FromBatch,
    Apply: Fn(EventHub, T) -> Applied,
    Applied: Future<Output = Result<()>>,
{
    loop {
        match subscription
            .next_frame()
            .await
            .context("read typed LaminarDB subscription")?
        {
            Some(TypedSubscriptionFrame::Rows { rows, .. }) => {
                for row in rows {
                    apply(hub.clone(), row).await?;
                }
            }
            Some(TypedSubscriptionFrame::Barrier { .. }) => {}
            None => bail!("subscription closed"),
        }
    }
}
