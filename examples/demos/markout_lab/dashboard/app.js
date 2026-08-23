"use strict";

const HORIZONS = [0, 1000, 5000, 15000, 30000];
const PLAYER_STORAGE_KEY = "laminardb-markout-player";
const playerId = getPlayerId();
const state = {
  feedPhase: "connecting",
  market: null,
  fills: new Map(),
  fillOrder: [],
  markouts: new Map(),
  curves: new Map(),
  summary: null,
  selectedFill: null,
  selectedHorizon: 0,
  orderBusy: false,
};

const byId = (id) => document.getElementById(id);
const eventSource = new EventSource(`/events?player_id=${encodeURIComponent(playerId)}`);

eventSource.onopen = () => {
  if (state.feedPhase === "live") byId("connection-dot").classList.add("connected");
};

eventSource.onerror = () => {
  state.feedPhase = "faulted";
  byId("connection-dot").classList.remove("connected");
  byId("connection-dot").classList.add("failed");
  byId("run-status").textContent = "LAB STOPPED · LIVE FEED REQUIRED";
  byId("phase-pill").textContent = "STOPPED";
  byId("phase-pill").classList.add("failed");
  setOrderMessage("The live connection closed. This demo does not substitute generated market data.", "error");
  updateOrderAvailability();
};

eventSource.addEventListener("status", (event) => {
  const status = JSON.parse(event.data);
  state.feedPhase = status.phase;
  const live = status.phase === "live";
  const failed = ["unavailable", "faulted"].includes(status.phase);
  byId("connection-dot").classList.toggle("connected", live);
  byId("connection-dot").classList.toggle("failed", failed);
  byId("run-status").textContent = live ? "LIVE MARKET CONNECTED" : (status.message || "MARKET UNAVAILABLE").toUpperCase();
  byId("phase-pill").textContent = live ? "LIVE" : status.phase.toUpperCase();
  byId("phase-pill").classList.toggle("waiting", status.phase === "connecting");
  byId("phase-pill").classList.toggle("failed", failed);
  if (live) setOrderMessage("Live market ready—choose BUY or SELL to start the experiment.", "");
  if (failed) setOrderMessage(status.message, "error");
  updateOrderAvailability();
});

eventSource.addEventListener("market", (event) => {
  state.market = JSON.parse(event.data);
  renderMarket();
  updateOrderAvailability();
});

eventSource.addEventListener("fill", (event) => {
  const fill = JSON.parse(event.data);
  if (fill.demo_run_id !== playerId) return;
  if (!state.fills.has(fill.fill_id)) state.fillOrder.push(fill.fill_id);
  state.fills.set(fill.fill_id, fill);
  state.selectedFill = fill.fill_id;
  state.selectedHorizon = 0;
  renderFills();
  renderSelectedFill();
});

eventSource.addEventListener("markout", (event) => {
  const markout = JSON.parse(event.data);
  if (markout.demo_run_id !== playerId) return;
  if (!state.markouts.has(markout.fill_id)) state.markouts.set(markout.fill_id, new Map());
  state.markouts.get(markout.fill_id).set(markout.horizon_ms, markout);
  if (state.selectedFill === markout.fill_id) state.selectedHorizon = markout.horizon_ms;
  renderFills();
  renderSelectedFill();
});

eventSource.addEventListener("curve", (event) => {
  const curve = JSON.parse(event.data);
  if (curve.demo_run_id !== playerId || curve.strategy !== "visitor") return;
  state.curves.set(curve.horizon_ms, curve);
  renderChart();
});

eventSource.addEventListener("summary", (event) => {
  const summary = JSON.parse(event.data);
  if (summary.demo_run_id !== playerId || summary.strategy !== "visitor") return;
  state.summary = summary;
  renderScorecard();
});

function getPlayerId() {
  const existing = window.sessionStorage.getItem(PLAYER_STORAGE_KEY);
  if (existing) return existing;
  const generated = window.crypto?.randomUUID?.() || `player-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
  window.sessionStorage.setItem(PLAYER_STORAGE_KEY, generated);
  return generated;
}

function renderMarket() {
  const market = state.market;
  byId("market-mid").textContent = market ? price(market.mid_px) : "—";
  byId("market-bid").textContent = market ? price(market.bid_px) : "—";
  byId("market-ask").textContent = market ? price(market.ask_px) : "—";
  byId("market-spread").textContent = market ? `${number(market.spread_bps, 3)} bps` : "—";
  byId("event-time").textContent = market ? timestamp(market.event_ts) : "—";
  byId("buy-button-price").textContent = market ? `at ${price(market.bid_px)}` : "waiting for market";
  byId("sell-button-price").textContent = market ? `at ${price(market.ask_px)}` : "waiting for market";
}

function updateOrderAvailability() {
  const enabled = state.feedPhase === "live" && Boolean(state.market) && !state.orderBusy;
  document.querySelectorAll("[data-side]").forEach((button) => {
    button.disabled = !enabled;
  });
}

async function placeOrder(side) {
  if (state.orderBusy || state.feedPhase !== "live" || !state.market) return;
  state.orderBusy = true;
  updateOrderAvailability();
  setOrderMessage("Binding your choice to the current live quote…", "");
  try {
    const response = await fetch("/api/orders", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        player_id: playerId,
        side,
        quantity: Number(byId("quantity-control").value),
      }),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || "The simulated order was rejected");
    setOrderMessage(`Accepted: simulated ${side} at ${price(result.simulated_fill_px)}. Watch the result unfold below.`, "success");
  } catch (error) {
    setOrderMessage(error.message, "error");
  } finally {
    state.orderBusy = false;
    updateOrderAvailability();
  }
}

function setOrderMessage(message, className) {
  const element = byId("order-message");
  element.textContent = message;
  element.classList.remove("success", "error");
  if (className) element.classList.add(className);
}

function renderSelectedFill() {
  const fill = state.fills.get(state.selectedFill);
  byId("selected-empty").hidden = Boolean(fill);
  byId("selected-content").hidden = !fill;
  if (!fill) return;

  byId("selected-side").textContent = fill.side;
  byId("selected-fill").textContent = price(fill.fill_px);
  byId("selected-quantity").textContent = `${number(fill.quantity, 4)} BTC`;
  byId("selected-time").textContent = timestamp(fill.event_ts);
  renderHorizonJourney(fill);

  const markout = state.markouts.get(fill.fill_id)?.get(state.selectedHorizon);
  const status = byId("selected-status");
  status.classList.remove("waiting", "failed");
  if (!markout) {
    status.textContent = "FILL CAPTURED · FUTURE PENDING";
    status.classList.add("waiting");
    setValue(byId("selected-outcome"), null, "usd");
    setValue(byId("selected-future"), null, "price");
    byId("selected-outcome-label").textContent = "CURRENT OUTCOME";
    byId("selected-horizon-label").textContent = "waiting for a future quote";
    byId("selected-explanation").textContent = "LaminarDB is waiting for the next eligible live-market moment.";
    return;
  }

  status.textContent = `${horizonLabel(markout.horizon_ms)} RESULT READY`;
  setValue(byId("selected-outcome"), markout.net_markout_pnl, "usd");
  setValue(byId("selected-future"), markout.future_mid_px, "price");
  byId("selected-outcome-label").textContent = `${horizonLabel(markout.horizon_ms)} HYPOTHETICAL OUTCOME`;
  byId("selected-horizon-label").textContent = `${horizonLabel(markout.horizon_ms)} after your fill`;
  byId("selected-explanation").textContent = markout.covered
    ? `LaminarDB matched your fill with the real market at ${horizonLabel(markout.horizon_ms)} and emitted this result.`
    : "No eligible future quote was available, so LaminarDB left this result unanswered.";
}

function renderHorizonJourney(fill) {
  const journey = byId("horizon-journey");
  journey.replaceChildren();
  for (const horizon of HORIZONS) {
    const markout = state.markouts.get(fill.fill_id)?.get(horizon);
    const card = document.createElement("button");
    card.type = "button";
    card.className = "horizon-card";
    card.classList.toggle("pending", !markout);
    card.classList.toggle("active", horizon === state.selectedHorizon);
    const title = horizon === 0 ? "AT THE FILL" : `${horizonLabel(horizon)} LATER`;
    const value = markout?.gross_markout_bps;
    card.innerHTML = `<span>${title}</span><strong>${value === null || value === undefined ? "pending" : `${signed(value, 2)} bps`}</strong><small>${markout ? "result arrived" : "waiting for market"}</small>`;
    card.disabled = !markout;
    card.addEventListener("click", () => {
      state.selectedHorizon = horizon;
      renderSelectedFill();
    });
    journey.append(card);
  }
}

function renderChart() {
  const rows = HORIZONS.map((horizon) => state.curves.get(horizon)).filter(Boolean);
  const group = byId("chart-series");
  group.replaceChildren();
  if (!rows.length) return;

  const values = rows.map((row) => row.weighted_net_markout_bps);
  const low = Math.min(0, ...values);
  const high = Math.max(0, ...values);
  const span = high === low ? 1 : high - low;
  const y = (value) => 180 - ((value - low) / span) * 156;
  const xByHorizon = new Map([[0, 52], [1000, 213], [5000, 374], [15000, 535], [30000, 696]]);
  byId("zero-line").setAttribute("y1", String(y(0)));
  byId("zero-line").setAttribute("y2", String(y(0)));

  const points = rows.map((row) => ({ row, x: xByHorizon.get(row.horizon_ms) }));
  group.append(svg("path", {
    d: points.map((point, index) => `${index ? "L" : "M"}${point.x},${y(point.row.weighted_net_markout_bps)}`).join(" "),
    class: "series-path",
  }));
  for (const point of points) {
    const pointY = y(point.row.weighted_net_markout_bps);
    group.append(svg("circle", { cx: point.x, cy: pointY, r: 4, class: "series-point" }));
    const label = svg("text", { x: point.x, y: pointY - 10, class: "point-value", "text-anchor": "middle" });
    label.textContent = signed(point.row.weighted_net_markout_bps, 2);
    group.append(label);
  }
}

function renderScorecard() {
  const summary = state.summary;
  setValue(byId("kpi-spread"), summary?.spread_capture_0s_bps, "bps3");
  setValue(byId("kpi-five"), summary?.weighted_markout_5s_bps, "bps");
  setValue(byId("kpi-pnl"), summary?.hypothetical_pnl_30s, "usd");
  setValue(byId("kpi-notional"), summary?.filled_notional, "notional");
  setValue(byId("kpi-adverse"), summary?.adverse_fill_rate_5s, "percent");
}

function renderFills() {
  const body = byId("fill-rows");
  body.replaceChildren();
  const visible = state.fillOrder.slice(-4).reverse();
  if (!visible.length) {
    const row = document.createElement("tr");
    row.className = "empty-row";
    row.innerHTML = '<td colspan="9">Your simulated fills will appear here.</td>';
    body.append(row);
    return;
  }
  for (const id of visible) {
    const fill = state.fills.get(id);
    const row = document.createElement("tr");
    row.dataset.fillId = id;
    row.classList.toggle("selected", id === state.selectedFill);
    row.append(cell(shortId(id)));
    row.append(cell(fill.side));
    row.append(cell(number(fill.quantity, 4)));
    row.append(cell(price(fill.fill_px)));
    for (const horizon of HORIZONS) {
      row.append(metricCell(state.markouts.get(id)?.get(horizon)?.gross_markout_bps));
    }
    row.addEventListener("click", () => {
      state.selectedFill = id;
      state.selectedHorizon = latestReadyHorizon(id);
      renderFills();
      renderSelectedFill();
    });
    body.append(row);
  }
}

function latestReadyHorizon(fillId) {
  const ready = HORIZONS.filter((horizon) => state.markouts.get(fillId)?.has(horizon));
  return ready.at(-1) ?? 0;
}

function setValue(element, value, kind) {
  element.classList.remove("positive", "negative", "pending");
  if (value === null || value === undefined) {
    element.textContent = "pending";
    element.classList.add("pending");
    return;
  }
  if (kind === "bps") element.textContent = `${signed(value, 2)} bps`;
  if (kind === "bps3") element.textContent = `${signed(value, 3)} bps`;
  if (kind === "usd") element.textContent = signedCurrency(value);
  if (kind === "notional") element.textContent = currency(value);
  if (kind === "percent") element.textContent = `${number(value, 1)}%`;
  if (kind === "price") element.textContent = price(value);
  if (["bps", "bps3", "usd"].includes(kind)) element.classList.add(value < 0 ? "negative" : "positive");
}

function metricCell(value) {
  const element = cell(value === null || value === undefined ? "pending" : signed(value, 1));
  element.className = value === null || value === undefined ? "pending" : value < 0 ? "negative" : "positive";
  return element;
}

function cell(value) {
  const element = document.createElement("td");
  element.textContent = value;
  return element;
}

function svg(name, attributes) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  return element;
}

function number(value, digits) { return Number(value).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits }); }
function signed(value, digits) { return `${value >= 0 ? "+" : ""}${number(value, digits)}`; }
function price(value) { return `$${number(value, 2)}`; }
function currency(value) { return `$${number(value, 0)}`; }
function signedCurrency(value) { return `${value >= 0 ? "+" : "-"}$${number(Math.abs(value), 4)}`; }
function timestamp(microseconds) { return `${new Date(microseconds / 1000).toISOString().slice(11, 19)} UTC`; }
function shortId(id) { return `#${id.split("-").at(-1)}`; }
function horizonLabel(value) { return value === 0 ? "fill time" : `${value / 1000}s`; }

document.querySelectorAll("[data-side]").forEach((button) => {
  button.addEventListener("click", () => placeOrder(button.dataset.side));
});

fetch("/api/pipeline").then((response) => response.text()).then((sql) => {
  byId("pipeline-sql").textContent = sql;
}).catch(() => {
  byId("pipeline-sql").textContent = "The executed SQL could not be loaded.";
});

if (new URLSearchParams(window.location.search).get("presentation") === "1") {
  document.body.classList.add("presentation");
}

renderMarket();
renderScorecard();
renderFills();
renderSelectedFill();
