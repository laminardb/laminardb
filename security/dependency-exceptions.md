# Temporary dependency exceptions

Status: approved 2026-09-21, re-approved 2026-10-21, zstd exceptions added 2026-10-27,
re-approved 2026-10-31, two unmaintained-crate exceptions added 2026-11-05, instant and
pin-utils exceptions removed 2026-11-10, chrono, chrono-tz and humantime exceptions added
2026-11-17, re-approved 2026-11-23 with the zstd exceptions removed; all expire at
2026-12-30 UTC (CI fails on that date).
Owner: LaminarDB maintainers responsible for [PR #600](https://github.com/laminardb/laminardb/pull/600).
The four residual exceptions below were re-reviewed and accepted for this interval under S4; the
chrono, chrono-tz and humantime exceptions joined them on 2026-11-17.
Machine-readable versions, sources, checksums and reasons are in `dependency-exceptions.json`.

| Advisory | Pinned crate | Accepted residual risk |
| --- | --- | --- |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | rsa 0.9.10 | No patched release. reqsign cloud authentication uses randomized signing rather than RSA decryption, but this does not establish absence of timing leakage or private-key exposure. |
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html) | paste 1.0.15 | Unmaintained compile-time dependency through tokenizers (laminar-db's `local` feature), which requires it directly in every release. |
| [RUSTSEC-2026-0173](https://rustsec.org/advisories/RUSTSEC-2026-0173.html) | proc-macro-error2 2.0.1 | Unmaintained compile-time dependency through validator_derive in Delta Lake. |
| [RUSTSEC-2026-0440](https://rustsec.org/advisories/RUSTSEC-2026-0440.html) | astral-tokio-tar 0.6.2 | Unmaintained since 2026-11-03, no maintained release. Test-only: reached through testcontainers 0.27.2 (0.28.0 still requires it), a laminar-connectors dev-dependency; no shipped build compiles it. |
| [RUSTSEC-2026-0542](https://rustsec.org/advisories/RUSTSEC-2026-0542.html) | humantime 2.3.0 | Unmaintained with chrono, no maintained release. Reached through deltalake-core, object_store 0.13 and humantime-serde (laminar-server's duration settings). |
| [RUSTSEC-2026-0543](https://rustsec.org/advisories/RUSTSEC-2026-0543.html) | chrono 0.4.45 | Unmaintained, no maintained release. A foundational dependency of Arrow, DataFusion, Parquet, iceberg, Delta Lake and the workspace. |
| [RUSTSEC-2026-0555](https://rustsec.org/advisories/RUSTSEC-2026-0555.html) | chrono-tz 0.10.4 | Unmaintained with chrono, no maintained release. Reached through arrow-array's time-zone support. |

Thirteen zstd advisories published in October 2026 (RUSTSEC-2026-0391 and 0395 to 0406) were
excepted from 2026-10-27 ([PR #637](https://github.com/laminardb/laminardb/pull/637)) to 2026-11-23,
when zstd 0.13.5 and zstd-safe 7.4.1 fixed them within the `^0.13` and `^7` lines that DataFusion,
Arrow, Parquet, iceberg, chitchat and apache-avro require. The lockfile now resolves those
releases, and zstd 0.14.3 and zstd-safe 8.2.0 for async-compression, so the exceptions are gone.

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
validated analytical dependency migration. Revisit the seven residual findings before expiry;
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
Re-review on 2026-11-10 (Arrow 59, DataFusion 55.2, Delta Lake 1.1): deltalake-catalog-unity 1.0
uses reqwest-retry 0.9, so instant and pin-utils left the lockfile and RUSTSEC-2024-0384 and
RUSTSEC-2026-0441 are no longer excepted. DataFusion 55.2 and Delta kernel 0.28 still require
object_store `^0.13.2` (so quick-xml `^0.39`), so the quick-xml patch and XML exceptions remain;
Arrow and Parquet 59.3 still require zstd `^0.13`.
Re-review on 2026-11-23: zstd 0.13.5 and zstd-safe 7.4.1 fix the thirteen zstd advisories within
the required major versions, so those exceptions are removed by a lockfile update. The rest have no
fix the graph can take. rsa 0.10.0 is now stable, but the latest reqsign-core 3.3.3,
reqsign-azure-storage 3.3.1 and reqsign-google 3.3.0 still require rsa `^0.9.2`. paste,
proc-macro-error2, astral-tokio-tar, humantime, chrono and chrono-tz have no maintained release.
DataFusion 55.2 still requires object_store `^0.13.2` (so quick-xml `^0.39`), so the quick-xml
patch and XML exceptions remain. The quick-xml backport's only change in this review is its due
date, so its reviewed digest was recomputed.
Re-review on 2026-11-26: no advisory was withdrawn or gained a patched release the graph can take.
reqsign-core 3.3.4, reqsign-azure-storage 3.3.2 and reqsign-google 3.4.0 still require rsa
`^0.9.2`. Every tokenizers release through 0.23.2 requires paste directly; DataFusion and Parquet no
longer reach it. Delta Lake 1.1.4 requires validator `^0.19`, whose derive uses proc-macro-error2
(validator_derive 0.20.1 drops it). testcontainers 0.28.0 still requires astral-tokio-tar `^0.6.1`.
DataFusion 55.2 still requires object_store `^0.13.2`, and object_store 0.13.2 is the last 0.13
release (quick-xml `^0.39`), so the quick-xml patch and XML exceptions remain. humantime, chrono
and chrono-tz have no maintained release.
Keep these exceptions under review before **2026-12-30**, which is the hard CI deadline.
