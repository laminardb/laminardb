# Stock-profile topology performance evidence

Original revision: `5d81ba9b18d80343373ecfaec4793df8c5caccf1`. The modified
queue binary is bound to `4cfcb2cd…` plus its exact recorded source changes.
Rust/Cargo 1.98.0 on Windows MSVC, unchanged locked dependencies, Ryzen 9 7900X,
12 cores/24 logical processors and 33,396,600,832 bytes RAM. All benchmark and
server executables use the existing optimized profiles, ThinLTO and one codegen
unit. No PE, stack or linker-profile patch was used.

The original server build verifies 1,177 Rust/Cargo inputs and the original
protobuf generator/schema bytes. Its successful stock build took 32m 56s and
the retained executable is `72a4ae46…`. A prior interrupted nested-cache build
and a generated-protobuf cache mismatch are retained under ignored
`target/topology-evidence`; neither is used as a successful executable.

## Queue benchmark

Both fresh retained binaries ran `accepted_push/arrow_16`, alternating original
then modified across three trials each. Each trial used two seconds of warmup,
five seconds of measurement, 100 samples and 10,000 bootstrap resamples. No
compiler or native soak ran during sampling. Raw samples, estimates, output and
binary/source identities are checked in beside `queue-comparison.json`.

| Measurement | Original | Modified |
| --- | ---: | ---: |
| Mean of three Criterion trial slopes, µs per burst | 6.05832 | 6.02855 |
| Relative modified point estimate | — | −0.49145% |

A burst admits and consumes all 32 shared prebuilt Arrow batches of 256 rows
each, through a capacity-64 queue on one current-thread runtime. This measures
queue and Arrow ownership cost for 8,192 referenced rows. It does not measure
production throughput, per-row latency or consumer-visible latency. Individual
trial confidence intervals remain in the estimate files; no production latency
guarantee or statistical claim is inferred from the average difference.

## Process comparison status

The original three-process steady scenario passed its final stateful, sink and
independent oracles in 228.05 seconds, using the retained corrected harness,
60 requested steady seconds, zero kills, 12 Kafka partitions, 400 offered
logical pairs/second, 4,096 keys, Zipf 1.2, 500 ms checkpoints and 64 key groups.
The same owned 4 GiB Kafka and S3 fixtures are retained. The matched modified
steady run passed all final oracles in 229.53 seconds with idle compilers. It used
the frozen 14-source `f9a05d5f…` stock server and the same `cbdf3983…` harness.
That server still fails public cold recovery; the subsequent repair is outside
these measurements. The original producer acknowledged 75,380 logical IDs at
400.0/s under its paced load; this is not a capacity measurement. All three graph
cycle histograms reported p50 ≤ 0.5 ms, p95 ≤ 1 ms and p99 ≤ 5 ms (bucket upper
bounds). Sampled combined RSS peaked at 932,233,216 bytes over the complete run.
The modified producer acknowledged 76,194 logical IDs at 400.0/s under the same
paced load. Full-run sampled combined RSS peaked at 805,085,184 bytes. All three
modified graph-cycle histograms reported p50 ≤ 0.5 ms and p99 ≤ 5 ms; nodes 0/1
reported p95 ≤ 1 ms and node 2 reported p95 ≤ 5 ms. Single-run bucket bounds and
varying cycle counts do not prove statistical equivalence or a capacity budget.
Allocation events and queue depth were unavailable; input-buffer bytes and
managed-state accounting are retained as separate gauges. The public migration
observations include the deliberate pause and remain correctness-oracle samples.

The original run's isolated namespace contained 325 objects and 122,135,388
stored bytes after completion, including 120,932,590 bytes in 23 state objects.
The inventory records the endpoint and categories; it is neither a reachability
proof nor a steady growth rate and grants no deletion authority.

The modified endpoint contained 323 objects and 98,887,986 stored bytes. Fresh
run namespaces and read-only listing hashes are recorded for both endpoints.
These are full-run observations, not a claim that topology roots or the bounded
request journal have been reclaimed.
