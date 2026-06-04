# Plan: LaminarDB Console UI (Admin / Operator Web UI for Cluster Mode)

- **Status:** Proposed (not started).
- **Date:** 2026-06-03
- **Scope:** A web console for LaminarDB (cluster mode first, works embedded too) that lets a
  user set up sources, author and run queries against sources/streams/MVs, view results in
  real time, build pipelines, and visualize pipeline lineage — in the spirit of RisingWave's
  dashboard, ksqlDB's editor, and Databricks Delta Live Tables.
- **Repo layout (decided):** the **UI is a separate repository** (static SPA). The **API it
  talks to is the existing `laminar-server` axum service, expanded into a control-plane API**
  — not a separate gateway. The control-plane API is server code that needs in-process access
  to `Arc<LaminarDB>` and the cluster control plane, so it belongs in this repo.
- **Realtime model (decided):** ad-hoc "watch this query live" registers an **ephemeral
  stream** and pushes it to the browser over **WebSocket/SSE**, matching the engine's
  streaming model. Point-in-time `SELECT` returns a JSON snapshot.
- **v1 scope (decided): "Author + observe."** Create sources/MVs/streams, run queries and see
  rows, tail data live, list/inspect objects. Pipeline-DAG visualization and the full cluster
  dashboard are Phase 2/3, not v1.

---

## How the reference systems do this (rationale)

- **ksqlDB** — a SQL editor over a REST API (`/ksql`, `/query-stream`). Statements return
  JSON; push queries stream rows over chunked HTTP / WS. The editor + streaming-result grid is
  the core UX. LaminarDB's `execute()` + `SUBSCRIBE` map directly onto this once HTTP returns
  rows.
- **RisingWave** — a dashboard that lists relations (sources, MVs, sinks) and renders the
  streaming **dependency/fragment graph**. The graph view (relation → relation edges, with
  per-fragment runtime stats) is its signature. This is the capability LaminarDB most lacks an
  API for today.
- **Databricks Delta Live Tables** — pipelines are a **first-class deploy-as-a-unit object**
  with a rendered DAG and per-table run state. LaminarDB pipelines are currently *implicit*
  (chained `CREATE STREAM`/`CREATE MATERIALIZED VIEW`/`CREATE SINK`); a first-class pipeline
  object is optional future work.

All three are: **(1) a SQL editor, (2) a relation browser, (3) a streaming results view, and
(4) a lineage graph.** LaminarDB has the engine primitives for 1–3 and needs the wiring for
all four to reach the browser.

---

## Current state (audited 2026-06-03)

### Client-facing interfaces that exist

| Interface | Location | Notes |
|---|---|---|
| HTTP REST (axum 0.8) | `crates/laminar-server/src/http.rs:38` (`build_router`) | `/health`, `/ready`, `/metrics`, `GET /api/v1/sources\|sinks\|streams`, `GET /api/v1/streams/{name}`, `POST /api/v1/sql`, `POST /api/v1/checkpoint`, `POST /api/v1/reload`, `GET /api/v1/cluster`, `GET /ws/{name}` |
| WebSocket | `http.rs:51`, `ws_upgrade`/`ws_client` `:426`–`:523` | Push subscription to a **pre-registered** stream; batches serialized via `arrow_json` as `{type:"data", subscription_id, data, sequence}`; 15s heartbeat. Backed by `db.subscribe_raw(&name)` (`db.rs:1387`). |
| pgwire (Postgres wire) | `crates/laminar-server/src/pgwire.rs` | Full SQL incl. `SELECT` (returns rows), `SUBSCRIBE` (realtime). Auth: MD5, pre-hashed `md5{...}`, TLS, optional mTLS, min-TLS-version. **The only interface that returns query rows today.** |
| Prometheus | `/metrics` (`http.rs:42`, `metrics::render`) | `laminardb_events_ingested_total`, `cycles_total`, `checkpoints_completed_total`, uptime, WS connections, etc. |

### SQL surface (already rich — reachable via `execute()` / pgwire)

- DDL: `CREATE SOURCE … FROM <CONNECTOR> (...)`, `CREATE SINK … FROM … INTO …`,
  `CREATE STREAM … AS SELECT`, `CREATE MATERIALIZED VIEW`, `CREATE LOOKUP TABLE`, `DROP …`.
  Generators showing exact grammar: `server.rs:422` (`source_to_ddl`), `:467`
  (`pipeline_to_ddl`), `:471` (`sink_to_ddl`), `:505` (`lookup_to_ddl`).
- Introspection: `SHOW SOURCES | SINKS | STREAMS | TABLES | MATERIALIZED VIEWS | QUERIES`,
  `DESCRIBE <source>`, `SHOW CHECKPOINT STATUS`, `SHOW CREATE SOURCE/SINK`
  (`laminar-sql/src/parser/tokenizer.rs:49`+, dispatched in `parser/mod.rs:128`+).
- Realtime: `SUBSCRIBE` (`parser/subscribe_parser.rs`), `RETAIN HISTORY` + `AS OF EPOCH`.
- Engine facade: `db.execute()`, `db.sources()`/`sinks()`/`streams()` (`db.rs:1884`+),
  `db.subscribe_raw()`, `db.checkpoint()`, `db.pipeline_state()`, `db.uptime()`.
  `laminar.models` / `laminar.ai_calls` system catalog views (`ai_catalog.rs`).

### Cluster control plane (rich internally, **unexposed**)

In `crates/laminar-server/src/cluster.rs` (`ClusterHandle`, `start_cluster`):
- Gossip membership with node states (`NodeState::Active|Draining|Suspected|…`) via
  `discovery.membership_watch()` (`cluster.rs:671`); `spawn_membership_watcher` already
  observes joins/leaves/state changes (`:59`).
- Vnode assignment: `AssignmentSnapshotStore` (versioned, CAS) (`cluster.rs:170`,
  `resolve_vnode_assignment` `:719`).
- Leader: `ClusterController` + `LeaderLeaseManager` (fenced lease, TTL) (`cluster.rs:640`).
- Rebalance controller + snapshot watcher (`cluster.rs:611`).
- Cluster-wide catalog manifest `CatalogManifestStore` (`cluster.rs:553`).

**All of this lives in `ClusterHandle` and is not reachable from the HTTP API.**
`GET /api/v1/cluster` (`http.rs:390`) returns only `{mode, node_id, pipeline_state}`.

---

## The gaps (what blocks a console)

### G0 — HTTP `POST /api/v1/sql` discards result rows  ★ blocker

`http.rs:305-314` maps `ExecuteResult::Query(_)` and `ExecuteResult::Metadata(_)` to a bare
`{result_type}` — **the rows are dropped.** `ExecuteResult::Query` holds a `QueryStream` and
`Metadata` a `RecordBatch` (`handle.rs:17`). Over HTTP today you cannot read the result of a
`SELECT` *or* of any `SHOW`/`DESCRIBE`. Until fixed, the UI is blind. (pgwire returns rows,
but browsers can't speak pgwire.)

### G1 — No browser realtime path for ad-hoc queries

`SUBSCRIBE` is pgwire-only; `/ws/{name}` only tails an already-registered stream. "Type a
SELECT, watch it live" has no endpoint.

### G2 — HTTP API has no authentication  ★ security blocker

`build_router` applies `CorsLayer::permissive()` (`http.rs:52`) and **no auth** on any route,
including `POST /sql`, `POST /checkpoint`, `POST /reload`. pgwire has full auth; the HTTP
control plane has none. A console must not ship the admin API unauthenticated.

### G3 — Cluster state not exposed

Membership, vnode assignment, leader/lease, rebalance, checkpoint status all exist in
`ClusterHandle` but aren't surfaced. `/api/v1/cluster` is a stub.

### G4 — No lineage / topology graph endpoint

There is an internal `OperatorGraph`, but nothing exposes source→stream→MV→sink edges. The
DAG visualization (the RisingWave/DLT signature) has no data source. Lineage is derivable by
parsing each object's SQL `FROM`/dependency refs (sqlparser already a dep; `visit_expressions`
used elsewhere).

### G5 — No connector catalog / form schema

A source-creation wizard needs the list of connector types and their required options to
render forms. Today that knowledge is implicit in the connector registry.

### G6 — Smaller REST gaps

MVs aren't listed over REST (only sources/sinks/streams); no `DROP` verbs; no "test/preview
source" before commit. `SHOW MATERIALIZED VIEWS` works via SQL once G0 lands.

> **Note:** G0–G3, G5 are *wiring existing engine capability to JSON*, not new engine
> features. G4 is light derivation. Only a first-class pipeline object (future) is real new
> engine surface.

---

## Architecture

```
Console UI (separate repo: React + TS + Vite SPA)
    │  HTTPS (REST, JSON rows)         WS / SSE (realtime push)
    ▼
laminar-server axum "control-plane API" (this repo, expanded http.rs)
    │  in-process
    ▼
Arc<LaminarDB>  +  ClusterHandle (membership_watch, AssignmentSnapshotStore,
                                  ClusterController, checkpoint status)
```

- Keep all backend work in `laminar-server`. The UI repo is a static SPA only.
- Emit an **OpenAPI spec** from the axum API (e.g. `utoipa`) and **generate the UI's typed
  client** from it so the two never drift.
- One node serves the console API; for cluster-wide answers it reads `ClusterHandle` state
  (already cluster-aware) rather than fanning out. Per-node metrics can be scraped from each
  node's `/metrics` later.

### Realtime design (ephemeral stream + WS/SSE)

1. UI `POST /api/v1/queries` with `{sql}` for an ad-hoc live query.
2. Server registers an **ephemeral, auto-named stream** (`CREATE STREAM __console_<uuid> AS
   <sql>`), returns `{stream_id, ws_url}`.
3. UI connects `GET /ws/{stream_id}` (existing path) and renders the batch stream.
4. On WS close / TTL, the server `DROP`s the ephemeral stream. (Add lifecycle/GC so abandoned
   ephemeral streams don't accumulate.)

This reuses `subscribe_raw` + the existing WS encoder. SSE is an alternative transport for the
same flow if WS is undesirable behind some proxies.

---

## UI repo: recommended stack

- **React + TypeScript + Vite** — static-deployable SPA.
- **SQL editor:** Monaco or CodeMirror 6.
- **Graph viz:** **React Flow** (best fit for the relation-graph look) — Phase 2.
- **Realtime:** native `WebSocket` (or `EventSource` for SSE).
- **Data:** TanStack Query; charts via Recharts/uPlot; embed Grafana for deep metrics rather
  than rebuilding them.
- **Client:** generated from the server's OpenAPI spec.

---

## Phased work

### Phase 0 — Server foundation (this repo) — prerequisite for any UI
- **G0:** `POST /api/v1/sql` returns rows. Add a JSON-rows response for `ExecuteResult::Query`
  (collect or paginate the `QueryStream`) and `ExecuteResult::Metadata`/`Ddl`. Reuse the
  Arrow→JSON path (`batch_to_json`, `http.rs:525`). Decide a row cap / pagination for large
  `SELECT`s.
- **G2:** Auth layer on the HTTP API (bearer token / session), and tighten CORS to an
  allow-list. Gate all `/api/v1/*` and `/ws`. Mirror the pgwire auth posture; reuse
  `pgwire_users` or add `[server].console_*` config.
- OpenAPI spec emission + CI check.

### Phase 1 — v1 "Author + observe" (UI repo + small server adds)
- SQL editor + results grid (depends on G0).
- Relation browser: sources / MVs / streams / sinks. **G6:** add `GET /api/v1/mvs`; surface
  `SHOW`/`DESCRIBE` results.
- Source-creation wizard. **G5:** `GET /api/v1/connectors` returning connector types +
  option schemas.
- Live tail of a registered stream over `/ws/{name}`.
- **G1:** ephemeral-stream realtime endpoint (`POST /api/v1/queries` → `ws_url`) + lifecycle
  GC, so "watch this SELECT live" works.

### Phase 2 — Pipeline visualization
- **G4:** `GET /api/v1/graph` → `{nodes, edges}` derived from object SQL dependencies.
- React Flow lineage view; optional per-node throughput/lag overlay from metrics.

### Phase 3 — Cluster dashboard
- **G3:** expose `ClusterHandle` state: `GET /api/v1/cluster/nodes` (members + `NodeState`),
  `/cluster/vnodes` (assignment, version), `/cluster/leader` (lease holder + fencing token),
  `/cluster/checkpoints` (`SHOW CHECKPOINT STATUS`), rebalance status.
- Node map, vnode-assignment heatmap, leader indicator, checkpoint/rebalance progress.

### Phase 4 — Optional / advanced
- First-class **pipeline** object (DLT-style deploy-as-a-unit) — real engine surface.
- Cluster **actions** (drain node, trigger rebalance) — write ops; need strict auth/audit.
- Per-object runtime metrics keyed by name for graph overlays.

---

## Risks / open questions

- **Row caps for `SELECT` over HTTP.** Unbounded materialization is unsafe; settle pagination
  vs. hard cap in G0.
- **Ephemeral-stream leakage.** Need robust GC tied to WS lifecycle + TTL (G1).
- **Per-node vs cluster-wide reads.** v1 reads cluster state from one node's `ClusterHandle`;
  per-node *metrics* aggregation across nodes is deferred (scrape each `/metrics`).
- **Auth model.** Reuse pgwire user store vs. a separate console identity? (G2) Decide before
  exposing write endpoints.
- **DROP semantics for live objects.** Dropping a source/MV with dependents needs clear
  errors / cascade rules in the API.

---

## Pointers

- HTTP router & handlers: `crates/laminar-server/src/http.rs`
- Server wiring / DDL generation: `crates/laminar-server/src/server.rs`
- Cluster startup & control plane: `crates/laminar-server/src/cluster.rs`
- pgwire (auth + SUBSCRIBE reference): `crates/laminar-server/src/pgwire.rs`
- Engine facade: `crates/laminar-db/src/db.rs`, `crates/laminar-db/src/handle.rs`
  (`ExecuteResult`), `crates/laminar-db/src/ddl.rs`
- SQL parser / SHOW commands: `crates/laminar-sql/src/parser/`
- System catalog views pattern: `crates/laminar-db/src/ai_catalog.rs`
