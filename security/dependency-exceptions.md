# Temporary dependency exceptions

Status: approved 2026-09-21, re-approved 2026-10-21, zstd exceptions added 2026-10-27,
re-approved 2026-10-31; all expire at 2026-11-30 UTC (CI fails on that date).
Owner: LaminarDB maintainers responsible for [PR #600](https://github.com/laminardb/laminardb/pull/600).
The four residual exceptions below were re-reviewed and accepted for this interval under S4.
Machine-readable versions, sources, checksums and reasons are in `dependency-exceptions.json`.

| Advisory | Pinned crate | Accepted residual risk |
| --- | --- | --- |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | rsa 0.9.10 | No patched release. reqsign cloud authentication uses randomized signing rather than RSA decryption, but this does not establish absence of timing leakage or private-key exposure. |
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html) | paste 1.0.15 | Unmaintained compile-time dependency through DataFusion/Parquet/tokenizers. |
| [RUSTSEC-2026-0173](https://rustsec.org/advisories/RUSTSEC-2026-0173.html) | proc-macro-error2 2.0.1 | Unmaintained compile-time dependency through validator_derive in Delta Lake. |
| [RUSTSEC-2024-0384](https://rustsec.org/advisories/RUSTSEC-2024-0384.html) | instant 0.1.13 | Unmaintained dependency in the wider lockfile, including reqwest-retry/wasm-timer and old parking_lot. |

Thirteen zstd advisories published in October 2026 were excepted on 2026-10-27 ([PR #637](https://github.com/laminardb/laminardb/pull/637)),
with the same deadline. They are fixed only in zstd 0.14.3 and zstd-safe 8.2.0, but DataFusion
55.2, Arrow and Parquet 58.4, iceberg 0.10.1, chitchat 0.13 and apache-avro 0.22 still require
zstd `^0.13`, so the lockfile pins zstd 0.13.3 and zstd-safe 7.3.0 for them. async-compression
0.4.51 (through compression-codecs 0.4.46) resolves the patched zstd 0.14.3 and zstd-safe 8.2.0,
which share the locked zstd-sys 2.1.2 (it fixes RUSTSEC-2026-0410); all four zstd identities are
pinned by checksum. The graph enables only zstd's default features (`legacy`, `arrays`,
`zdict_builder`) and none of zstd-safe's `experimental`, `seekable` or `zstdmt`:

| Advisories | Status |
| --- | --- |
| [0395](https://rustsec.org/advisories/RUSTSEC-2026-0395.html), [0396](https://rustsec.org/advisories/RUSTSEC-2026-0396.html), [0398](https://rustsec.org/advisories/RUSTSEC-2026-0398.html), [0399](https://rustsec.org/advisories/RUSTSEC-2026-0399.html), [0402](https://rustsec.org/advisories/RUSTSEC-2026-0402.html), [0404](https://rustsec.org/advisories/RUSTSEC-2026-0404.html), [0406](https://rustsec.org/advisories/RUSTSEC-2026-0406.html) | Not compiled or not reachable: the APIs need `experimental`, `seekable` or a `zstdmt` multithreaded context. |
| [0391](https://rustsec.org/advisories/RUSTSEC-2026-0391.html), [0397](https://rustsec.org/advisories/RUSTSEC-2026-0397.html), [0400](https://rustsec.org/advisories/RUSTSEC-2026-0400.html), [0401](https://rustsec.org/advisories/RUSTSEC-2026-0401.html), [0405](https://rustsec.org/advisories/RUSTSEC-2026-0405.html) | No caller found: context reuse across streams, `OutBuffer::around_pos` (whose only caller, compression-codecs, now uses zstd-safe 8.2.0), dictionaries, dictionary training and an out-of-range `InBuffer` position are not used on zstd-safe 7.3.0. Workspace code calls only `zstd::bulk`. |
| [0403](https://rustsec.org/advisories/RUSTSEC-2026-0403.html) | Accepted residual risk: with `legacy` on, a failed streaming decompression of a legacy frame from untrusted input (DataFusion's synchronous compressed-file reads, Parquet, Iceberg and Avro data) can expose uninitialized memory in the decoder's output buffer. |

Remove these exceptions when DataFusion, Arrow, Parquet, iceberg, chitchat and apache-avro accept
zstd 0.14 in the pinned analytical generation.

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
Re-review on 2026-10-31: RUSTSEC-2026-0381 is no longer excepted, because the advisory now lists
the webpki-roots 0.26.11 compatibility package as unaffected and neither scanner reports it.
No other exception has a fix the graph can take. rsa 0.10 is still at 0.10.0-rc.19, and
reqsign-google and reqsign-azure-storage (OpenDAL's GCS and Azure services) enable it. paste,
proc-macro-error2 and instant have no new release. Even the latest DataFusion 55.2.0 and Delta Lake
1.1.3 still require object_store `^0.13.2` (so quick-xml `^0.39`), Delta Lake still requires
validator `^0.19` (whose derive uses proc-macro-error2), and iceberg-storage-opendal 0.10.1 still
requires OpenDAL `^0.57`. instant comes only through parking_lot 0.11, which wasm-timer requires
through reqwest-retry 0.7 in deltalake-catalog-unity 0.16; unity 1.0 moves to reqwest-retry 0.9
but needs the Delta Lake 1.x generation. The quick-xml backport's only change in this review is
its due date, so its reviewed digest was recomputed.
Keep these exceptions under review before **2026-11-30**, which is the hard CI deadline.
