# Temporary dependency exceptions

Status: approved 2026-09-21, re-approved 2026-10-21, expires at 2026-11-20 UTC (CI fails on that date).
Owner: LaminarDB maintainers responsible for [PR #600](https://github.com/laminardb/laminardb/pull/600).
The four residual exceptions below were re-reviewed and accepted for this interval under S4.
Machine-readable versions, sources, checksums and reasons are in `dependency-exceptions.json`.

| Advisory | Pinned crate | Accepted residual risk |
| --- | --- | --- |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | rsa 0.9.10 | No patched release. reqsign cloud authentication uses randomized signing rather than RSA decryption, but this does not establish absence of timing leakage or private-key exposure. |
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html) | paste 1.0.15 | Unmaintained compile-time dependency through DataFusion/Parquet/tokenizers. |
| [RUSTSEC-2026-0173](https://rustsec.org/advisories/RUSTSEC-2026-0173.html) | proc-macro-error2 2.0.1 | Unmaintained compile-time dependency through validator_derive in Delta Lake. |
| [RUSTSEC-2024-0384](https://rustsec.org/advisories/RUSTSEC-2024-0384.html) | instant 0.1.13 | Unmaintained dependency in the wider lockfile, including reqwest-retry/wasm-timer and old parking_lot. |

[RUSTSEC-2026-0381](https://rustsec.org/advisories/RUSTSEC-2026-0381.html) (webpki-roots 0.26.11)
is excepted as a scanner match, not an accepted risk. That release bundles no certificates: it
re-exports `TLS_SERVER_ROOTS` from webpki-roots 1.x, which the lockfile pins at the patched
1.0.10 (both versions are pinned by checksum). Only async-nats reaches 0.26, through
tokio-websockets 0.10; every async-nats release up to 0.50 still requires it, and building
without its `websockets` feature would remove `ws://` NATS server URLs. Remove this exception
when async-nats moves to tokio-websockets 0.12 or later.

Two additional exceptions cover RUSTSEC-2026-0194 and RUSTSEC-2026-0195, with the same deadline.
Workspace builds still require the verified quick-xml backport. Registry publication now has
explicit acceptance of the **unpatched XML CPU and memory denial-of-service risks** because
Cargo does not propagate workspace patches to consumers. This is a temporary release exception,
not a claim that the published dependency graph is repaired. Upstream provenance is in
[`vendor/quick-xml/SECURITY-BACKPORT.md`](../vendor/quick-xml/SECURITY-BACKPORT.md).

Before either scanner, CI verifies expiry, exact lockfile identities and checksums, the
local patch path, its entire source digest, and agreement between both scanner ignore lists.
Changing any accepted version or source requires a new review; other advisories retain their
existing severity. Local verification must likewise run the guard before either scanner:

```sh
python tools/check_dependency_exceptions.py
cargo audit --deny warnings
cargo deny --locked --workspace check
```

Scope: embedded, single-node server and cluster builds from this workspace use the XML patch.
Published library consumers do **not** inherit Cargo workspace patches. Consumers using the
affected cloud storage or connectors must apply the same backport in their application's root manifest
or use a compatible dependency graph with quick-xml >=0.41; the published dependency graph
is not qualified by this workspace-only repair. The release workflow permits registry publication
only while the explicit XML publication exception is current; missing acceptance or expiry still
blocks upload. `--publication` on the guard checks that exception without uploading anything.
Remove the patch and XML exceptions when the
current object_store/OpenDAL generation accepts a fixed release, or after a separately
validated analytical dependency migration. Revisit the four residual findings before expiry;
there is no automatic extension.

Re-review on 2026-09-21: crates.io still lists object_store 0.13.2 as the latest 0.13 release,
requiring quick-xml `^0.39.0`; the advisory fixes require `>=0.41.0`. OpenDAL's pinned 0.57
generation also remains behind its latest 0.59 generation. Even current DataFusion 55.1.0 and
Delta Lake 1.0.0 require object_store `^0.13.2`; the latest iceberg-storage-opendal 0.10.1
requires OpenDAL `^0.57`. Merely updating direct dependencies would leave the affected versions
in the transitive graph and introduce incompatible storage types. See the registry dependency
records for [DataFusion](https://crates.io/api/v1/crates/datafusion/55.1.0/dependencies),
[Delta Lake](https://crates.io/api/v1/crates/deltalake-core/1.0.0/dependencies), and
[Iceberg's adapter](https://crates.io/api/v1/crates/iceberg-storage-opendal/0.10.1/dependencies).
paste, proc-macro-error2 and instant
still have no maintained compatible release; RSA's next generation remains a release candidate.
Re-review on 2026-10-21: rsa, paste, proc-macro-error2 and instant still have no new stable
release (rsa 0.10 is at 0.10.0-rc.19). object_store 0.14.2 now accepts quick-xml `^0.41.0`, but
current DataFusion 55.2.0 and Delta Lake 1.1.2 still require object_store `^0.13.2`, and
iceberg-storage-opendal 0.10.1 still requires OpenDAL `^0.57` (latest 0.59.5), so the patch and
XML exceptions remain until the analytical dependency migration. The backport's only change in
this review is its due date, so its reviewed digest was recomputed.
Keep these exceptions under review before **2026-11-20**, which is the hard CI deadline.
