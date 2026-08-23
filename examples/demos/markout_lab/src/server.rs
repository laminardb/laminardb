use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_stream::stream;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::engine::{EngineHealth, EngineMonitor};
use crate::orders::{OrderError, OrderService, PlaceOrderRequest};
use crate::state::{EventHub, FeedPhase, FeedStatus, UiEvent};

const INDEX_HTML: &str = include_str!("../dashboard/index.html");
const APP_JS: &str = include_str!("../dashboard/app.js");
const STYLES_CSS: &str = include_str!("../dashboard/styles.css");

#[derive(Clone)]
pub struct ServerState {
    hub: EventHub,
    orders: OrderService,
    engine: EngineMonitor,
    pipeline_sql: Arc<str>,
}

impl ServerState {
    #[must_use]
    pub fn new(
        hub: EventHub,
        orders: OrderService,
        engine: EngineMonitor,
        pipeline_sql: Arc<str>,
    ) -> Self {
        Self {
            hub,
            orders,
            engine,
            pipeline_sql,
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    engine_backed: bool,
    live_market_data: bool,
    simulated_orders_only: bool,
    engine: EngineHealth,
    feed: FeedStatus,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
struct PlayerQuery {
    player_id: String,
}

pub async fn serve(host: IpAddr, port: u16, state: ServerState) -> Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(javascript))
        .route("/styles.css", get(stylesheet))
        .route("/events", get(events))
        .route("/api/health", get(health))
        .route("/api/pipeline", get(pipeline))
        .route("/api/orders", post(place_order))
        .with_state(state);
    let address = SocketAddr::new(host, port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("bind Markout Lab to {address}"))?;
    tracing::info!(url = %format_args!("http://{address}"), "Live Markout Lab ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve Markout Lab")
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn javascript() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn stylesheet() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLES_CSS,
    )
}

async fn pipeline(State(state): State<ServerState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        state.pipeline_sql.to_string(),
    )
}

async fn health(State(state): State<ServerState>) -> Json<HealthResponse> {
    let engine = state.engine.snapshot();
    let feed = state.hub.status().await;
    let live = feed.phase == FeedPhase::Live;
    Json(HealthResponse {
        ok: engine.state == "Running" && live,
        engine_backed: true,
        live_market_data: live,
        simulated_orders_only: true,
        engine,
        feed,
    })
}

async fn events(State(state): State<ServerState>, Query(query): Query<PlayerQuery>) -> Response {
    if !valid_player_id(&query.player_id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid player_id".to_string(),
            }),
        )
            .into_response();
    }
    let player_id = query.player_id;
    let mut receiver = state.hub.subscribe();
    let initial = state
        .hub
        .snapshot_events(&player_id)
        .await
        .unwrap_or_else(|error| {
            vec![serialization_error_event(format!(
                "Could not build reconnect snapshot: {error:#}"
            ))]
        });
    let hub = state.hub.clone();
    let events = stream! {
        for event in initial {
            yield Ok::<_, Infallible>(to_sse(event));
        }
        loop {
            match receiver.recv().await {
                Ok(event) if event.visible_to(&player_id) => yield Ok(to_sse(event)),
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    match hub.snapshot_events(&player_id).await {
                        Ok(snapshot) => {
                            for event in snapshot {
                                yield Ok(to_sse(event));
                            }
                        }
                        Err(error) => {
                            yield Ok(to_sse(serialization_error_event(format!(
                                "Could not recover lagged event stream: {error:#}"
                            ))));
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(10))
                .text("markout-lab"),
        )
        .into_response()
}

fn to_sse(event: UiEvent) -> Event {
    Event::default().event(event.name).data(event.data)
}

fn serialization_error_event(message: String) -> UiEvent {
    let data = serde_json::json!({
        "phase": "faulted",
        "message": message,
    })
    .to_string();
    UiEvent::global("status", data)
}

async fn place_order(
    State(state): State<ServerState>,
    Json(request): Json<PlaceOrderRequest>,
) -> Response {
    match state.orders.place(request).await {
        Ok(receipt) => (StatusCode::ACCEPTED, Json(receipt)).into_response(),
        Err(error) => order_error_response(error),
    }
}

fn order_error_response(error: OrderError) -> Response {
    let status = match error {
        OrderError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        OrderError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        OrderError::PlayerLimit => StatusCode::CONFLICT,
        OrderError::FeedUnavailable | OrderError::PlayerCapacity | OrderError::Engine(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    (
        status,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
        .into_response()
}

fn valid_player_id(player_id: &str) -> bool {
    (8..=64).contains(&player_id.len())
        && player_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "could not install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    tracing::warn!(%error, "could not install terminate handler");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            () = ctrl_c => {}
            () = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}
