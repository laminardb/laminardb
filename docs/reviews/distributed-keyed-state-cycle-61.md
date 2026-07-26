# Distributed keyed state Cycle 61 review

- **Date:** 2026-07-26
- **Scope:** content-bound prebuilt-server launch seam and separate-observer proportionality audit
- **Code outcome:** accepted in `a3c2e0f9` and `c834d28e`; integration-test harness only
- **Empirical outcome:** no real-process soak, A/B, backend trial, or candidate execution ran
- **Runtime/backend outcome:** no runtime dependency, state backend, operator admission, evidence
  route, source/sink contract, or delivery guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, complete
  checkpoint authority, powered instrumentation equivalence, and the independently operated
  immutable release-binary soak remain open

## Executable identity boundary

Both ignored real-process tests previously launched only Cargo's compile-time
`CARGO_BIN_EXE_laminardb`. They now resolve one explicit executable identity before side effects:

- neither override variable selects and hashes the existing Cargo-built test binary;
- `LAMINAR_SOAK_LAMINARDB_EXE` and `LAMINAR_SOAK_LAMINARDB_SHA256` must otherwise both be present;
- the override path must be absolute, then canonicalize to a regular file; paths remain `OsString`/
  `PathBuf` throughout so Windows spaces, UNC paths, and verbatim canonical paths work;
- the declared digest is exactly 64 lowercase hexadecimal characters and must match a streamed
  SHA-256 of the selected bytes; and
- partial/empty configuration, relative/missing/directory paths, malformed digests, mismatch, and
  post-resolution byte changes fail closed without fallback.

Every process generation requires a private, non-cloneable verification permit. Creating the
permit reopens and rehashes the canonical path, while `Node::spawn` consumes it and verifies that
its shared identity is the node's selected identity. All five initial/restart call sites require a
permit. Local restart permits are acquired before the existing kill-to-recovery timer and cluster
rejoin permits before the existing rejoin timer, so hash I/O is outside those measurements.
Successful receipts record node, PID, digest, and, for the Kafka cluster path, process generation.
The child is installed into `Node` before fallible accounting or receipt output so `Drop` can clean
it up after a panic.

This closes accidental cross-generation substitution in a controlled soak. It does not defend
against a hostile same-user replacement in the remaining portable verify/exec window. The runner
must stage the artifact read-only and retain pre/post hashes. Rehashing warms filesystem cache and
uses host I/O during a run; any result is therefore a common-harness, cache-warmed measurement, not
cold-start evidence. Copying binaries, changing ACLs, or adding OS-specific `fexecve`/handle launch
logic was rejected as disproportionate and potentially incorrect for adjacent DLLs/shared objects.

## Observer boundary decision

No observer or schedule-only helper was added. The existing harness remains structurally
ineligible for the Cycle 60 A/B: exact evidence finalization gates the kill, steady evidence reads
shift loop timing, and final evidence gates producer shutdown and correctness validation. Letting it
launch another binary changes provenance, not those feedback paths.

A standalone schedule generator without a consuming driver would be exercised only by its own
tests. It could not prove that observer failure or output is ignored during the measured window and
would be unused scaffolding. The next code slice must therefore land a capability-limited external
observer together with a driver dry run. The driver must precompute its checkpoint/fault/end plan,
must not read or wait for observer output before the fixed end boundary, and must produce the same
plan when the observer exits, hangs, or emits malformed artifacts.

The executable seam also does not turn the current target into the independent production soak.
Its hard-kill placement relies on a debug/test-only checkpoint gate; an ordinary immutable release
binary cannot exercise that path merely by being selected through the override.

## Cycle review

- **AI slop — pass:** no coupled-harness run was relabelled A/B or independent evidence; no
  self-contained schedule helper was created to manufacture a completed checkbox.
- **Overengineering and hot path — pass with declared cache effect:** all code is private to the
  integration-test target. There is no server runtime toggle, product dependency, row/checkpoint
  branch, binary copy, ACL mutation, or platform-specific exec layer. Hashing precedes RTO clocks,
  but its cache warming is retained as a limitation.
- **Unused code — pass:** both ignored soaks call the resolver, every `Node` spawn consumes a permit,
  and the pure resolver is covered directly. Observer code was deferred until a driver consumes it.
- **Production readiness — NO-GO:** the code is provenance scaffolding only. No distributed keyed
  operator runs, TidesDB remains stopped before integration, exactly-once composition is absent,
  and the independent soak has not run.
- **Documentation — pass:** the production-soak charter owns invocation and threat-model details;
  this review records the decision. `git ls-files -- docs/research .claude` returned no tracked
  research or Claude-memory artifact, so there was nothing to remove in this cycle.
- **Tests — pass for the implemented slice:** four focused resolver tests passed; cluster-only
  coverage passed 11 non-ignored tests with one real-process test ignored; Kafka-feature coverage
  passed 38 non-ignored tests with two real-process tests ignored; warnings-denied Clippy passed
  for cluster-only and Kafka-feature targets; formatting and diff checks passed.

## Cycle 62 review plan

1. **AI slop:** implement the observer only with a consuming deterministic driver dry run; do not
   claim a dry run measures perturbation or production latency.
2. **Overengineering/hot path:** keep the observer outside runtime crates and give it no process-
   launch, checkpoint, kill, Kafka, or correctness authority. Do not add a runtime feature toggle.
3. **Unused code:** require the driver to consume one canonical plan/artifact schema and prove the
   same immutable workload/fault/end plan for observer success, exit, hang, and malformed output.
4. **Production readiness:** keep A/B v1 effect estimation, future powered equivalence v2, and the
   independent release-binary soak as three separate gates.
5. **Documentation:** retain driver, observer, manifest, schedule, executable, and configuration
   hashes in one dry-run record; do not duplicate the Cycle 60 numerical protocol.
6. **Tests:** prove C/D base-schedule identity, zero control HTTP connections, bounded treatment
   request order/retries/cursors, secret redaction, exclusive artifacts, and no-feedback behavior.
