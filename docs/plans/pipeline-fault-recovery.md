# Pipeline Fault Recovery

A pipeline submitted over the HTTP API that panics an operator on the compute
thread used to set terminal `DbState::Stopped`: `/ready` stuck at 503, the reason
logs-only, and `start()` refused to recover — only a process restart cleared it.

## Implemented (manual recovery)

- `DbState::Faulted` — recoverable, distinct from intentional `Stopped`. The
  compute-thread watcher sets it on an unexpected exit (`pipeline_lifecycle.rs`).
- The panic message is captured into `LaminarDB::last_fault` and surfaced via
  `GET /api/v1/pipeline/status` (`last_error`) and the `/ready` 503 body;
  `pipeline_faults_total` counts crashes.
- `start()` recovers from `Faulted` by rebuilding from the surviving catalog, so
  an operator drops the offending object and restarts in-process. The connector
  DDL guard already permits drops while `Faulted`.
- Manual policy (no auto-retry) so a deterministic bad query can't crash-loop.
  `/health` stays 200 for `Faulted` (alive + recoverable); only `/ready` goes
  503, so an orchestrator won't kill the pod mid-recovery.

## Open follow-ups

- **Bounded auto-restart** (opt-in): retry `start()` N times with backoff for
  transient faults, then park in `Faulted`. Config-gated.
- **Cluster-health review**: a `Faulted` node reports unready but keeps gossip
  membership `Active`, so rebalance (membership-driven) won't auto-shed its
  vnodes — recovery is operator-driven. Confirm this is the desired behavior vs
  auto-shed before it matters in a multi-node deployment; auto-shed risks
  reassignment storms when a poisoned MV faults every node.
- **Per-operator isolation**: wrap each operator's `process()` so one bad query
  is quarantined instead of taking down the whole pipeline. Large; pairs with
  the hot-remove machinery in `hot-add-connector-while-running.md`.
