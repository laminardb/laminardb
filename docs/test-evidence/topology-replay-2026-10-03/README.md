# Subscription replay and retention across migration, 2026-10-03

This increment starts at `79c7a7a88650d1ee1d8a9eeaac7d7b21b3881a41` on
`feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Eleven changed Rust sources are
frozen in [source-identity.json](source-identity.json); Cargo.lock is unchanged.

An unchanged subscription can cross a pipeline identity change only through
exact immutable roots of participant-complete released migrations. Every other
certificate field retains strict equality. Historical manifests, references,
segment bindings and hashes retain their original identities. This authorizes
artifact reads only. Readers cache the selected historical certificate within a
pipeline; migration audits remain at checkpoint boundaries, outside row delivery.

Retention validates each exact predecessor and projects the historical
certificate roster using those same roots. Future-only generations are absent
from older inventories. Cleanup requires the complete horizon reference, including
its digest and length. Missing or corrupt evidence stops traversal before deletion.
Migration state/root pins remain protected; root consumption and journal reclamation
are subsequent work.

Four new tests use actual stored Arrow output segments. An old tail reader attached
before the target checkpoint and a target-certificate `AS OF EPOCH 1` reconnect
receive identical partition sequence IDs and aggregate 45. Incarnation/schema
changes return their explicit errors; query/retention contract changes fail exact
root matching. Retention crosses the original root, preserves all referenced
segments, and rejects an altered horizon. Deleting the sealed root stops replay
and cleanup without deleting target output. Original installation/Release is an
authority fixture; this is reader/retention evidence, not broker or multi-process
activation certification.

The focused four cases pass. The standalone DB suite passes 2,019 tests using
`--locked -p laminar-db --no-default-features --features cluster,aws,kafka --lib`
with eight test threads and the unchanged 4 MiB test stacks. This standalone
feature union differs from the prior four-package union; the earlier 4,446-test
result is not counted as a new run. All-target Clippy over the four-package union,
minimal server, cluster/FFI, formatting and diff/source checks are recorded in
[build-checks.txt](build-checks.txt). [unit-results.txt](unit-results.txt) contains
the exact commands and test summaries. The initial Clippy semicolon warning was
corrected before the frozen test run. Native MSVC links retain the existing
OpenSSL LNK4099 warnings; the focused native build took 18m08s.

Public SQL/atomic submission, safe removals, root/journal reclamation, the real
multi-process migration/restart oracle and comparative performance remain ongoing.
