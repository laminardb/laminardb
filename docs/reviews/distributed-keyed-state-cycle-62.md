# Distributed keyed state Cycle 62 review

- **Date:** 2026-07-27
- **Scope:** standalone C/D schedule scaffold and observer-failure isolation
- **Code outcome:** accepted in `70ed0327`; root-workspace-excluded validation tool only
- **Empirical outcome:** no SUT, HTTP, A/B, backend trial, real-process cluster run, or soak ran
- **Runtime/backend outcome:** no LaminarDB crate, runtime dependency, state backend, admission rule,
  source/sink contract, checkpoint path, or delivery guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, complete
  checkpoint authority, powered instrumentation equivalence, exactly-once composition, and the
  independently operated immutable release-binary soak remain open

## Accepted scaffold

[`tools/distributed-state-ab`](../../tools/distributed-state-ab/) is an unpublished standalone
workspace with two distinct binaries and a committed lockfile. Its direct normal dependencies are
`serde`, `serde_json`, and `sha2`; there is no LaminarDB, async-runtime, network, Kafka, candidate,
or backend dependency.

The strict manifest binds driver, observer, server, trace, declared-redacted configuration, dependency,
virtual-control, and protocol artifacts by canonical regular-file path, length, and SHA-256. The
common plan includes the raw manifest hash, which also binds limits, no-credential authentication,
and C/D mapping. Arm and injected child behavior are CLI-only. The driver/observer byte identities
must differ, artifact paths cannot alias, unknown and credential-bearing manifest fields fail closed, and
the official treatment JSON is preflighted against its stdout cap.

The driver schedule has 104 declarations: start, checkpoints 1--80 at 1.5-second slots with the
fault declaration at 120 seconds, input target end at 200 seconds, checkpoints 81--101 from 256.5
seconds, and end at 290 seconds. The observer schedule has 58 slots from zero through 285 seconds,
three nodes, and two route labels: 348 planned probes. Four lifecycle labels are schedule anchors,
not observed runtime transitions. C suppresses the probes and D serializes them; neither opens a
connection or implements HTTP, retries, pagination, cursors, response parsing, workload execution,
fault actuation, or wall-clock timing.

The child receives an empty environment and a fixed one-way stdin signal. Stdout/stderr pipe bytes
are drained concurrently into capped memory, so they cannot grow the driver's retained output
artifacts beyond the caps or deadlock the driver on a full pipe. No receiver result, status, or
interpretation is exposed to the driver schedule path. The driver first validates the complete materialized trace against the plan, writes
it exclusively, and file-syncs it. Only then can a private, non-cloneable, plan-bound end seal be
created; collection consumes it by value. Status checks, bounded kill/reap, capture reception,
output classification, and output artifact creation follow. A true cleanup failure retains the PID
and `TerminationFailed`; a pre-collection driver failure invokes the same bounded cleanup and logs
the PID if manual cleanup is required. The base plan and trace are reread before the final record.

The `{C,D} x {success,exit,hang,malformed}` integration matrix requires byte-identical plan and
trace artifacts and hashes. Spawn failure also leaves the schedule trace unchanged. This proves a
logical property of the reviewed scaffold, not OS-level isolation. A same-user child can inspect
accessible processes/files, path identity retains verify/open and verify/spawn TOCTOU, capture
threads consume CPU/I/O, and several referenced artifacts are opaque hashes rather than executed
inputs.

## Live-observer stop condition

The current HTTP protected router applies the console bearer to diagnostic GETs and mutation
routes, including checkpoint, SQL/reload, and pipeline start/stop. Giving it to a polling observer
would also give that observer control authority. Cycle 62 therefore has no token, socket, HTTP
client, parser, or live endpoint.

A live observer is blocked until owners approve and review a server-enforced route-scoped
read-only credential or an equivalently enforceable GET-only broker. A code-convention-only broker
that still holds an unrestricted bearer is not least authority. The observer must then receive a
sanitized plan rather than the full artifact manifest, and a loopback fake server must prove exact
origin/method/path, zero C connections, deadlines, retry ceilings, page/cursor transitions,
response bounds, process identity, and failure behavior before any cluster is contacted.

## Independent reviews

Two independent reviews initially found material faults: the old end token was cloneable/reusable,
output went to uncapped files, kill/reap could block, the observer learned a writable marker path,
manifest reads had a growth race, an end-boundary probe was impossible, and unimplemented HTTP/
retry/page semantics were overstated. All were corrected or removed before acceptance.

The final contract review accepted the private file-synced end seal, bounded capture, raw-manifest
binding, honest planned-probe vocabulary, and 285-second last slot. The final Windows/process
review accepted PID retention, repeated bounded kill/reap, the parked-child test, the separately
bound 100--60,000-ms completion budget, package isolation, and dependency hygiene. Both explicitly
limited the result to schedule/non-feedback mechanics.

## Cycle review

- **AI slop — pass with narrow claim:** the CLI prints `SCHEDULE_SCAFFOLD_DRY_RUN_OK` only after a
  single run and every structured schema says `NOT A/B OR CERTIFICATION EVIDENCE`. Counterfactual identity is
  asserted only by the test matrix. No static probe declaration is described as an HTTP request,
  timing sample, workload execution, or A/B result.
- **Overengineering and hot path — pass with size noted:** the new tool is sizeable because it owns
  strict schemas, content identity, process supervision, and hostile outcomes, but remains outside
  the root workspace and all product paths. Live HTTP, a generic orchestration framework, OS
  sandboxing, server auth changes, and runtime toggles were deferred. No row, checkpoint, state,
  source, or sink hot path changed.
- **Unused code — pass:** both binaries are invoked by the consuming CLI matrix; every manifest and
  plan field affects validation, identity, scheduling, output bounds, or the final record. The
  intentionally ignored parked-child function is only a subprocess fixture invoked by its active
  reaper test. Opaque referenced artifacts are explicitly provenance-only, not falsely called
  executed inputs.
- **Production readiness — NO-GO:** no distributed keyed operator or TidesDB backend ran; no source/
  state/sink atomicity, exactly-once, failover, latency, memory, hot-key, checkpoint, or restore
  result exists. The independent immutable release-binary soak remains mandatory and unrun.
- **Documentation — pass:** the soak charter owns the detailed boundary; ADR, plan, and validation
  report link to it without duplicating Cycle 60's statistical protocol. `git ls-files --
  docs/research .claude` returned no tracked research or Claude-memory artifact, so there was no
  obsolete tracked research to delete.
- **Tests — pass for the scaffold:** `cargo test --locked --manifest-path
  tools/distributed-state-ab/Cargo.toml --all-targets` passed 10 tests with one intentionally ignored
  subprocess fixture. Strict all-target Clippy, formatting, release-bin build, normal dependency
  audit, and diff checks passed. Overlapping test invocations briefly left the first invocation
  holding the Windows driver executable, so the second hit `Access is denied`; after the first
  ended, no package process remained and the serial rerun passed.

Coverage debt remains for injected start-signal failure, subprocess oversized output, true OS kill
failure, cross-plan end-seal rejection, and forced trace-persistence failure through `Drop`. These
do not block the explicitly ineligible scaffold; they must close before a live driver is eligible.

## Cycle 63 review plan

1. **AI slop:** select and document one enforceable diagnostic-read authority; do not describe an
   unrestricted bearer inside a convention-only broker as least privilege.
2. **Overengineering/hot path:** prefer a server-enforced route scope on the HTTP control plane;
   keep it off event/state/checkpoint hot paths and do not build the live client yet.
3. **Unused code:** first land a threat model and route/auth matrix consumed by tests or a concrete
   implementation plan; add no speculative token parser, proxy, or client.
4. **Production readiness:** keep live polling, the effect-estimation A/B, powered equivalence, and
   independent release-binary soak as separate later gates.
5. **Documentation:** specify rotation/reload, redaction, route matching, loopback/remote exposure,
   downgrade, audit, and failure semantics once in the ADR/charter.
6. **Tests:** before live use, require route-matrix tests proving the read-only credential succeeds
   only on the two diagnostic GETs and fails on every mutation route, query-token path, WebSocket,
   unknown path, and method substitution; then add the loopback observer protocol tests.
