use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;

use crate::engine::EngineInputs;
use crate::state::EventHub;
use crate::types::{SimulatedFill, FILL_MODEL};

const MAX_ACTIVE_PLAYERS: usize = 128;
const MAX_ORDERS_PER_PLAYER: u16 = 20;
const ORDER_COOLDOWN: Duration = Duration::from_millis(500);
const MIN_QUANTITY: f64 = 0.000_1;
const MAX_QUANTITY: f64 = 0.1;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PlaceOrderRequest {
    pub player_id: String,
    pub side: OrderSide,
    pub quantity: f64,
}

#[derive(Debug, Serialize)]
pub struct OrderReceipt {
    pub accepted: bool,
    pub fill_id: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub simulated_fill_px: f64,
    pub source_event_id: i64,
    pub source_event_ts: i64,
    pub fill_model: &'static str,
}

#[derive(Debug, Error)]
pub enum OrderError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("live market feed is unavailable or stale; no simulated order was created")]
    FeedUnavailable,
    #[error("wait briefly before placing another simulated order")]
    RateLimited,
    #[error("this browser has reached the limit of {MAX_ORDERS_PER_PLAYER} simulated orders")]
    PlayerLimit,
    #[error("the live demo has reached its active-player capacity")]
    PlayerCapacity,
    #[error("LaminarDB rejected the simulated fill input: {0}")]
    Engine(String),
}

struct PlayerQuota {
    orders: u16,
    last_order: Instant,
}

#[derive(Clone)]
pub struct OrderService {
    inputs: EngineInputs,
    hub: EventHub,
    stale_after: Duration,
    maker_fee_bps: f64,
    next_fill: Arc<AtomicU64>,
    players: Arc<Mutex<BTreeMap<String, PlayerQuota>>>,
}

impl OrderService {
    #[must_use]
    pub fn new(
        inputs: EngineInputs,
        hub: EventHub,
        stale_after: Duration,
        maker_fee_bps: f64,
    ) -> Self {
        Self {
            inputs,
            hub,
            stale_after,
            maker_fee_bps,
            next_fill: Arc::new(AtomicU64::new(1)),
            players: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Bind a visitor action to the latest engine-emitted live quote.
    ///
    /// # Errors
    /// Fails closed for invalid input, unavailable market data, bounded quota,
    /// or an engine input failure.
    pub async fn place(&self, request: PlaceOrderRequest) -> Result<OrderReceipt, OrderError> {
        validate_request(&request)?;
        let market = self
            .hub
            .latest_market(self.stale_after)
            .await
            .ok_or(OrderError::FeedUnavailable)?;
        validate_market(&market)?;
        self.reserve_player_order(&request.player_id)?;

        let fill_number = self.next_fill.fetch_add(1, Ordering::Relaxed);
        let fill_id = format!("{}-{fill_number:04}", request.player_id);
        let fill_px = match request.side {
            OrderSide::Buy => market.bid_px,
            OrderSide::Sell => market.ask_px,
        };
        let fill = SimulatedFill {
            demo_run_id: request.player_id.clone(),
            fill_id: fill_id.clone(),
            source_event_id: market.sequence,
            strategy: "visitor".to_string(),
            symbol: market.symbol,
            side: request.side.as_str().to_string(),
            quantity: request.quantity,
            fill_px,
            fee_bps: self.maker_fee_bps,
            mid_at_fill: market.mid_px,
            spread_bps_at_fill: market.spread_bps,
            decision_reason: format!(
                "Visitor requested a simulated passive {} at the live touch",
                request.side.as_str()
            ),
            fill_model: FILL_MODEL.to_string(),
            event_ts: market.event_ts,
        };
        if let Err(error) = self.inputs.push_fills(std::slice::from_ref(&fill)).await {
            self.release_player_order(&request.player_id);
            return Err(OrderError::Engine(format!("{error:#}")));
        }

        Ok(OrderReceipt {
            accepted: true,
            fill_id,
            side: request.side,
            quantity: request.quantity,
            simulated_fill_px: fill_px,
            source_event_id: fill.source_event_id,
            source_event_ts: fill.event_ts,
            fill_model: FILL_MODEL,
        })
    }

    fn reserve_player_order(&self, player_id: &str) -> Result<(), OrderError> {
        let now = Instant::now();
        let mut players = self.players.lock();
        if let Some(player) = players.get_mut(player_id) {
            if player.orders >= MAX_ORDERS_PER_PLAYER {
                return Err(OrderError::PlayerLimit);
            }
            if now.duration_since(player.last_order) < ORDER_COOLDOWN {
                return Err(OrderError::RateLimited);
            }
            player.orders += 1;
            player.last_order = now;
            return Ok(());
        }
        if players.len() >= MAX_ACTIVE_PLAYERS {
            return Err(OrderError::PlayerCapacity);
        }
        players.insert(
            player_id.to_string(),
            PlayerQuota {
                orders: 1,
                last_order: now,
            },
        );
        Ok(())
    }

    fn release_player_order(&self, player_id: &str) {
        let mut players = self.players.lock();
        let remove = if let Some(player) = players.get_mut(player_id) {
            player.orders = player.orders.saturating_sub(1);
            player.orders == 0
        } else {
            false
        };
        if remove {
            players.remove(player_id);
        }
    }
}

fn validate_request(request: &PlaceOrderRequest) -> Result<(), OrderError> {
    if !(8..=64).contains(&request.player_id.len())
        || !request
            .player_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(OrderError::InvalidRequest(
            "player_id must be 8-64 ASCII letters, digits, hyphens, or underscores".to_string(),
        ));
    }
    if !request.quantity.is_finite() || !(MIN_QUANTITY..=MAX_QUANTITY).contains(&request.quantity) {
        return Err(OrderError::InvalidRequest(format!(
            "quantity must be between {MIN_QUANTITY} and {MAX_QUANTITY} BTC"
        )));
    }
    Ok(())
}

fn validate_market(market: &crate::types::MarketEvent) -> Result<(), OrderError> {
    if !market.bid_px.is_finite()
        || !market.ask_px.is_finite()
        || market.bid_px <= 0.0
        || market.ask_px < market.bid_px
    {
        return Err(OrderError::FeedUnavailable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(player_id: &str, quantity: f64) -> PlaceOrderRequest {
        PlaceOrderRequest {
            player_id: player_id.to_string(),
            side: OrderSide::Buy,
            quantity,
        }
    }

    #[test]
    fn request_bounds_are_explicit() {
        assert!(validate_request(&request("player-123", 0.001)).is_ok());
        assert!(validate_request(&request("short", 0.001)).is_err());
        assert!(validate_request(&request("player-123", 1.0)).is_err());
        assert!(validate_request(&request("player<script>", 0.001)).is_err());
    }
}
