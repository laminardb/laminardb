# Pipeline Fault Recovery — a bad pipeline must not brick the node

Addresses issue #2 from the dynamic-source debugging session: a pipeline
submitted over the HTTP API that **panics an operator on the compute thread**
leaves the node permanently unhealthy with no error surfaced to the operator.

## 1. Current behavior (what actually happens)

The compute thread (`pipeline_lifecycle.rs:1677-1716`) runs the
`StreamingCoordinator` under `catch_unwind`:

```
let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt.block_on(coordinator.run(callback))));
if let Err(panic) = result {
    let msg = panic.downcast_ref::<String>()... ;          // 1698-1703
    tracing::error!(panic = msg, "laminar-compute thread panicked");
    return;                                                  // done_tx dropped
}
done_tx.send(());
```

On panic the message is **logged only**, `done_tx` is dropped, and the watcher
task fires:

```
if done_rx.await.is_err() {                                 // 1731
    tracing::error!("laminar-compute thread exited unexpectedly");
    DbState::Stopped.store(&watcher_state);                 // terminal
    watcher_shutdown.notify_one();
}
```

Then:
- `pipeline_state()` → `"Stopped"`; `/ready` returns **503** forever
  (`http.rs:419-437`).
- `pipeline_status` (`http.rs:923`) returns only the state string — **no
  reason**.
- `start()` on `Stopped` **errors**: *"Cannot start a stopped pipeline. Create a
  new LaminarDB instance."* (`pipeline_lifecycle.rs:196-199`) → the only
  recovery is a **process restart**.

So a single bad submitted query bricks the node, and the cause is buried in
logs. In a cluster that node is dead weight until someone bounces it.

## 2. Goals

1. **Don't brick** — a crashed pipeline is *recoverable* without a process
   restart: the operator drops/fixes the offending object and restarts.
2. **Surface the reason** — the panic message is retrievable via the API
   (`/api/v1/pipeline/status`, health) and metrics, not just logs.
3. **Fail safe in a cluster** — a faulted node reports unready (correct) and the
   failure does not cascade into rebalance storms.

## 3. Design

### 3.1 Capture the fault (low risk, do first)
Add `last_fault: Arc<Mutex<Option<PipelineFault>>>` on the db, where
`PipelineFault { message: String, at_ms: i64 }`. Set it in the compute thread on
panic (the message is already extracted at `:1698`) before the thread returns,
and/or in the watcher. Add a `pipeline_faults_total` counter + a
`pipeline_faulted` gauge to `EngineMetrics`.

### 3.2 Recoverable state instead of terminal `Stopped`
Introduce a distinct `DbState::Faulted` (don't overload `Stopped`, which means
*intentional* terminal shutdown). The watcher sets `Faulted` (not `Stopped`) on
unexpected exit. `start()` gains a `Faulted` arm that behaves like `Created`
(rebuilds the coordinator from the surviving `ConnectorManager`/catalog
registrations — those live on the db, not the dead thread). `stop()`/`shutdown()`
still go to `Stopped`.

Recovery flow: node `Faulted` → operator reads the reason → `DROP` the bad
stream/MV/source (these handlers don't need a running pipeline) → `POST
/api/v1/pipeline/start` → coordinator rebuilds without the offending object.

### 3.3 Surface via API
- `pipeline_status` → `{ "pipeline_state": "Faulted", "last_error": "<msg>",
  "faulted_at": <ms> }`.
- `/ready` stays 503 when not `Running`, but include the reason in the body.
- `/health` distinguishes `Faulted` from `Stopped`.
- The *original* DDL/start call already returned before the async panic, so the
  reason is delivered through status/health/metrics, not that call's response.
  (Synchronous DDL/plan errors are still returned inline as today.)

### 3.4 Restart policy — THE DESIGN FORK
A data-dependent panic (a specific bad row) will re-crash on restart from the
same offset → a crash loop. Two options:

- **(A) Manual (recommended default).** Faulted node stays put; no auto-retry.
  Operator inspects, fixes/drops, restarts. Safest — never crash-loops, never
  masks a deterministic bug.
- **(B) Bounded auto-restart.** Supervisor retries `start()` up to
  `pipeline_restart_max_attempts` with exponential backoff; after N fast
  consecutive faults it parks in `Faulted` and surfaces. Good for transient
  faults, risky for deterministic ones.

Recommendation: ship **(A)** first; add **(B)** as opt-in config later
(`[server] pipeline_restart_max_attempts`, `pipeline_restart_backoff_ms`).

### 3.5 Cluster health interaction (needs review)
A `Faulted` node should report **unready** (503) so LBs route away, but keep
gossip membership `Active` so it isn't declared dead. Rebalance is
membership-driven, not readiness-driven, so a faulted-but-alive node will **not**
auto-shed its vnodes — recovery is operator-driven. Confirm this against the
rebalance controller + leader lease before implementing; the alternative
(auto-shed on fault) risks reassignment storms if the fault is cluster-wide
(e.g. a poisoned MV replayed on every node).

### 3.6 Stretch: per-operator fault isolation
The coordinator runs all operators in one loop (`operator_graph`). Wrapping each
`process()` in `catch_unwind` and quarantining the offending operator (drop its
MV/stream, keep the rest running) would contain a bad query to itself instead of
taking down the whole pipeline. Much larger change; deferred. Tracks with the
hot-remove machinery in `hot-add-connector-while-running.md`.

## 4. Phasing
1. **Capture + surface** (§3.1, §3.3) — no behavior change, just observability.
2. **Faulted state + manual recovery** (§3.2, policy A).
3. **Cluster-health review** (§3.5) + opt-in auto-restart (policy B).
4. **Per-operator isolation** (§3.6) — stretch.

## 5. Validation
Needs a deterministic panic to test. Add a test-only source/operator (behind a
test cfg/hook) that panics on a sentinel value, then assert: state → `Faulted`,
`last_fault` populated, `/ready` 503 with reason, `pipeline_faults_total`
incremented, and `DROP` + `start()` recovers to `Running`.
