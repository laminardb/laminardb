# state-tier-bench

Benchmark gate for the state cold tier (`docs/plans/tiered-operator-state.md`,
Phase 0). Exercises [fjall](https://github.com/fjall-rs/fjall) with workloads
shaped like demoted operator state and reports the numbers the go/no-go
decision needs. Deliberately a standalone crate (own lockfile): fjall must not
enter the main workspace until the gate passes.

## Modes

- `--mode group` — point upserts/reads of `(operator, vnode, group-key) →
  accumulator-state` records (default 2M keys × 256 B). Models the v2
  group-granularity design.
- `--mode slice` — whole `(operator, vnode) → serialized-slice` blobs
  (default 256 × 4 MiB), with fjall key-value separation enabled. Models the
  v1 slice-granularity design.

Writes are Zipfian-skewed (`--zipf-s`, default 0.99) and paced to
`--write-rate` ops/s; reads sample keys uniformly from a dedicated thread —
i.e. dominated by keys the writer rarely touches, mirroring cold promotion
fetches running off the compute thread.

## Gate criteria

Run on target-class **Linux NVMe** (write amplification and CPU come from
`/proc`; they print `n/a` elsewhere):

1. cold-read p99 ≤ 1 ms under sustained write load,
2. background (compaction) CPU bounded,
3. write amplification acceptable at the sustained upsert rate.

## Running the gate

The dataset must be **well beyond RAM**, or the OS page cache makes every
read warm and the p99 is fiction. On a 64 GB box:

```bash
# group mode: ~96 GB logical (400M × 240B values + keys), 30 min steady state
cargo run --release -- --path /nvme/stb --keys 400000000 --value-bytes 240 \
  --duration-secs 1800 --write-rate 200000

# slice mode: ~96 GB logical (24576 × 4 MiB), slice rewrites at 50/s
cargo run --release -- --path /nvme/stb --mode slice --keys 24576 \
  --duration-secs 1800 --write-rate 50
```

Record the output of both modes in the tiered-state plan / ADR when flipping
its status. If the LSM fails the gate, the named fallback is a purpose-built
Bitcask/FASTER-style hash-log.

## Caveats

- Short, small runs (like CI smoke) are cache-resident and always "pass" —
  they only prove the harness works.
- `write amplification` counts all process writes (journal + flush +
  compaction) during the steady window via `/proc/self/io`, divided by the
  logical bytes the writer produced. Run long enough for compaction to reach
  steady state or it underestimates.
- The process CPU figure includes the writer/reader threads; subtract the
  load you'd expect from the op rates to estimate the background share.
