# Evidence ledger: brand refresh and public copy (2026-10-18, updated 2026-10-19 for v0.33.0)

This records what the rebranded website, READMEs and Console say, where each statement comes from,
and how it was checked. It is a review aid for this change, not user documentation.

## Revisions and release state

| Item | Value |
| --- | --- |
| Engine changes | branch `feat/channel-identity` from `3c9e057c` (= `origin/main` = tag `v0.33.0`). The local `main` checkout (`8bc09242`, 3 commits behind) was not touched. |
| Console changes | `laminardb-console-ui` branch `feat/channel-identity` from `de90132` (= `origin/main`, tree-identical to local `feature/console-v0.32` `e6d6c64`). |
| Installable release | v0.33.0 (`3c9e057c`): GitHub release assets for 6 targets + `SHA256SUMS` (all six checksums verified); crates.io `laminar-db` 0.33.0; Docker Hub `laminardb/laminardb-server:0.33.0` (pushed 2026-10-19 05:21 UTC, image label revision `3c9e057c`). |
| Previous release | v0.32.0 (`907d3dfa`); its assets, crate and image remain published. The first 0.33.0 tag lacked the `v` prefix and produced nothing; it was replaced by `v0.33.0` on 2026-10-19. |
| Docker `latest`/`main` | Rebuilt from `main` on every push (`docker.yml`: `type=raw,value=latest,enable={{is_default_branch}}`). Not a release. |
| Client packages | PyPI `laminardb` 0.17.0 (uploaded 2026-03-01); npm `@laminardb/node` 0.30.0-alpha.4; Maven `io.laminardb:laminardb:0.30.0-alpha` (engine v0.30.0, `features = ["api"]`). |

Release facts were read with `gh release view`, the crates.io, PyPI, npm, Maven Central and Docker
Hub APIs, and `docker image inspect` on 2026-10-19 (client packages unchanged since 2026-10-18).

## Claims

Levels: **code** (implemented), **docs** (documented), **local** (run here against the released
v0.33.0 image unless a row says v0.32.0), **CI**, **release** (in published artifacts).

| Claim (where) | Evidence | Level | Qualification kept with the claim |
| --- | --- | --- | --- |
| Open-source, Apache 2.0 engine license (site facts, README) | `LICENSE`; `Cargo.toml` `license = "Apache-2.0"`; image label | code, release | Python bindings are MIT (not claimed as Apache). |
| Written in Rust; Apache Arrow data; DataFusion query engine (site facts) | workspace `Cargo.toml` (Arrow 58.4, DataFusion 53.1); sources emit `arrow_array::RecordBatch` (e.g. `generator/mod.rs` `build_batch`) | code | Versions not printed on the site. |
| "Process streaming data with SQL" / reads events, runs SQL continuously, writes to a sink or keeps it ready to query (hero) | quick start and Kafka example below | local | — |
| Tumbling, hopping, session windows close on event time when the watermark passes the window end (site row 01) | `docs/SQL_REFERENCE.md` windows section; `laminar-sql` window rewriter; Kafka example output | docs, code, local | — |
| Connector list (site row 02, README table, `llms.txt`) | registry `crates/laminar-db/src/db/mod.rs`; `laminar-server` default features; `GET /api/v1/connectors` on 0.33.0 (identical on 0.32.0) lists sources `delta-lake, files, generator, iceberg, kafka, mongodb-cdc, nats, otel, postgres-cdc, websocket` and sinks `delta-lake, files, iceberg, kafka, mongodb-sink, nats, postgres-sink, websocket` | code, local, release | RabbitMQ, IBM MQ, FIX and FpML described as optional build features, with the per-binary split in the prebuilt-binaries row; WebSocket and OpenTelemetry sources best-effort only. |
| Materialized views queryable over HTTP; embedded/single-node only (site row 03, README) | quick start run; `ddl/materialized_view.rs` cluster rejection (LDB-4007) | local, code | Stated on the row. |
| Checkpoints and restart from last committed checkpoint; exactly-once needs replayable source + checkpoint-committed sink (site row 04, `llms.txt`) | `crates/laminar-connectors/README.md` delivery sections; `connector/contracts.rs`; validation LDB-0014 | docs, code | Other pairs at-least-once; single-node replay needs `file://`. Not run end-to-end here. |
| Cluster: shared S3/GCS/Azure checkpoints; Kafka-only sources; no MVs or AI functions (site Deployment, README Modes) | `pipeline_lifecycle/source_admission.rs:63-76` (only Kafka is splittable); `config/validation.rs` (no `file://`); `ddl/cluster_checks.rs` | code | Cluster not run here. |
| Embedded crate checkpoints to a local directory (site, README) | `laminar-db/src/builder/mod.rs` (`storage_dir`; LDB-0014 admits replay-capable local delivery only with a `file://` checkpoint, cloud stores only with best-effort); crates.io 0.33.0 | code, release | No Rust example published; not compiled here. |
| Single server: HTTP SQL, WebSocket subscriptions, Prometheus, optional PostgreSQL wire (site) | `laminar-server/src/http/router.rs`; `pgwire_bind` off by default; `laminardb --help` (identical on 0.32.0 and 0.33.0) | code, local | `SUBSCRIBE` over pgwire only via cursor/portal (README). |
| Quick start (site, README) | README blocks extracted and run verbatim on 2026-10-19 against `0.33.0`: healthy within 4 s, `rows_seen` 41 at 4 s rising about 10 per second (earlier run on `0.32.0`: 82 → 125) | local | Uses `-p 127.0.0.1:8080:8080`. |
| Kafka per-minute volume example and its output (site, README) | README blocks run verbatim with `apache/kafka:4.1.1` and `0.33.0` on 2026-10-19 (and `0.32.0` on 2026-10-18); output identical to the README both times (3 rows; open 09:32 window) | local | Needs `'startup.mode' = 'earliest'` under at-least-once (verified error otherwise). |
| Prebuilt binaries for Linux, macOS, Windows (README, docs) | v0.33.0 asset list | release | Linked by tag so the commands survive later releases. |
| Which v0.33.0 binaries include RabbitMQ, IBM MQ, FIX and FpML (README, docs, `llms.txt`) | each archive unpacked and run on 2026-10-19 with `GET /api/v1/connectors` and the README FpML example (14 rows each where FpML is present): x86_64 Linux glibc in `ubuntu:24.04` lists `rabbitmq`, `ibmmq`, `fix`; aarch64 Linux in emulated arm64 `ubuntu:24.04` and Windows natively list `fix`; x86_64 Linux musl in `alpine:3.20` lists none and rejects `FORMAT FPML`; the 0.33.0 image rejects `FORMAT FPML` and lists none; macOS binaries not run, but their strings show the FIX and FpML markers and none for RabbitMQ or IBM MQ, matching the other binaries; `release.yml` build flags | local, release, CI | macOS by string check and `release.yml` only. Every archive, including musl, ships the FIX and FpML notices in `licenses/`. |
| Helm chart; set `image.tag` (README) | `deploy/helm/laminardb/values.yaml` (`tag: ""` → appVersion); live index lists only chart 0.33.0 | code, release | Its default image `0.33.0` exists since 2026-10-19 05:21 UTC; chart publishing is open issue 2. |
| Console: browser app for a server you run, uses HTTP API with console token (site, READMEs) | Console `src/api.ts`, contract doc; live captures | code, local | Hosted build is static; nothing hosted for users. |
| Hosted Console reaches a local server only with the browser's local-network permission (site note, Console README) | Live `https://laminardb.github.io/laminardb-console-ui/` against the quick-start server on `127.0.0.1:8080`, Chrome 155 headless: blocked ("Permission was denied ... loopback address space") by default; "Connected and ready · v0.32.0" after granting `local-network-access` | local | Other browsers not tested. |
| The image exits at startup without `LAMINAR_CONSOLE_TOKEN` (README) | `docker run --rm laminardb/laminardb-server:0.33.0` (and `0.32.0`) → `Error: missing environment variables: LAMINAR_CONSOLE_TOKEN` | local | — |
| The Console connects to a v0.33.0 server and shows a compatibility notice (README) | branded Console dev build against the `0.33.0` quick-start server: "Connected and ready · v0.33.0" and "This console was reviewed against LaminarDB 0.32.0 (bd8f85e8); the connected server reports 0.33.0. Protocol compatibility is not guaranteed." (`verification/console/after/connected-0.33.0-server.png`) | local | Console contract not re-reviewed for 0.33.0; only `crates/laminar-server/src/config/tests.rs` changed in the server crate between the pin and `v0.33.0`. |
| Client library versions (README) | registry queries above; Python `Cargo.toml` path dependency | release | Stated as lagging the engine. |
| AI functions list and limits (README, docs) | `sql_analysis/ai/mod.rs:214-225`, `ai_udf.rs:99-106`, `cluster_checks.rs:325-329` | code | One aliased call per query, cluster rejected, ONNX Runtime ≥ 1.24 not in image. Not run here. |
| pre-1.0 status (site, README) | workspace version 0.x | code | No production-readiness claim. |

Removed from the site because they were unsupported, stale or fabricated: benchmark numbers
(4.24 ns, 235.9M/s, 383 ns, 2.65M/s, 451.1 µs, 40.27 ms: no hardware, revision or method on the
page), "millisecond-latency", "distributed MPP scaling", "START FOR FREE", the mock Console panel
with "24.5k eps", connector "Stable/Preview" badges, the "Ring 0/1/2" architecture (contradicted
by the current runtime model), the `releases/latest` download script, `cargo install laminar-server`,
`helm --set cluster.enabled/persistence.size` (keys do not exist), version 0.25.0, the
"streaming engine that starts embedded" site description, and the FAQ/FAQPage schema.

## Reference inputs and component map

All five references matched `reference-manifest.json` (SHA-256 and size) and were opened as images.

| Reference | Observed | Destination |
| --- | --- | --- |
| `desktop-1440.png` | 100 px gutters, 100 px header with 1 px rule, H1 ≈73 px bold at -0.055 em, 18/30 px lede, 48 px buttons (radius 4), fact strip between rules at y 686/737, numbered rows ~95 px apart, footer band `#E9EEE8` | `site/index.html`, `site/css/style.css` |
| `mobile-390.png` | 20 px gutters, 84 px header (Docs + GitHub only), H1 ≈43 px, stacked natural-width buttons, illustration after text, 2×2 fact grid | responsive rules in `site/css/style.css` |
| `wordmark-check.png` | two colours only (`#162C29`, `#2D695E`); mark 135×127 units at 1.125 px; Liberation Sans Regular lettering, -0.04 em | `brand/source/*.svg`; site header; README and Console logos |
| `channel-mark-detail.png` | three channels, stroke 14, gap 22, flat tops, shared right edge, concentric turns with outer radii 14/28/56 | `brand/source/laminardb-mark.svg`; favicon, avatar, app icons, collapsed Console header |
| `hero-flow-detail.png` | 5 inputs, 2 outputs, 21.6 px grid, accent event markers 14×7, ink result markers 32×12, ink block with three mint (`#A7D0C1`) channel lines | `brand/source/conceptual-flow.svg`, inlined in the home page |

## Intentional departures from the references

- Header lettering uses the canonical regular-weight wordmark; the page renders show heavier HTML
  text. `wordmark-check.png` governs letterforms.
- Hero paragraph, platform lede and rows use verified copy; the paragraph is 3 lines (reference 2),
  and the platform list has 5 rows (reference 3). Sections were added for how it works,
  deployment and the quick start in the same visual language.
- The caption reads "Conceptual data flow"; "Not a Console screenshot" was dropped as internal
  reassurance. The reference's Console sentence about simulated dashboards was not used.
- Arrows are drawn as small SVGs (↓ for in-page links) because the Latin font subsets lack arrow
  glyphs. A theme toggle sits in the header (the site already had light/dark/system themes).
- The header logo starts at the content edge (x = 100) rather than x = 106.
- Fonts are Arimo and Cousine (self-hosted, OFL), metric-compatible with the Liberation fonts the
  references were rendered in; no claim that they are the originals.

## Screenshot provenance

The only product screenshot published is `laminardb-console-ui/docs/images/console-lineage.png`
(provenance in `docs/images/README.md`: 0.32.0 image, demonstration Kafka trades and the
built-in generator, Lineage tab, light theme, 1440 × 900, Chrome 155 headless). The site uses no
product screenshots. Verification captures are kept outside the repositories in
`laminardb-design-handoff-v2/verification/`.

## Similarity review (scope and limits)

- Text searches on 2026-10-18 for the headline "Process streaming data with SQL" found it used as a
  generic description across the streaming-SQL category, as expected for a plain description; it is
  not presented as a slogan.
- The retired line "Data flows. So do ideas." is not present in any text in the engine, Console or
  Python repositories, or in the site images; it was not replaced.
- Logo searches for nested L / rounded-corner marks found no specific match. The closest named
  result, Linedata (financial software), was rendered from its own SVG: a red rounded "D" in a
  teardrop, structurally unlike the channel mark.
- Not performed: reverse-image search, trademark-register searches (USPTO, EUIPO, WIPO) and legal
  review. Nothing here establishes originality, exclusivity, registrability or clearance, and rights
  in the LaminarDB name are outside the scope of a visual change.

## Checks run

| Check | Result |
| --- | --- |
| Console `npm test` / `npm run lint` / `npm run build` | 51/51 tests pass; ESLint clean; type-checked build succeeds |
| Console build with `GITHUB_ACTIONS=1` served under `/laminardb-console-ui/` | all assets, favicons and touch icon return 200 |
| axe-core (WCAG 2.0–2.2 A/AA) on 10 Console states, live 0.32.0 server, 1440 px | 0 violations (before: 7 states with violations) |
| axe at 1024 px | 0; at 390 px: pre-existing `scrollable-region-focusable` on table/chart wrappers (open issue) |
| axe (WCAG + best practices) on home, docs, 404, light and dark, 1440 and 390 | 0 violations |
| Keyboard order on home | skip link → logo → nav → theme → GitHub → buttons → rows → code; 2 px focus ring on every stop |
| Horizontal overflow at 390/768/1024/1440 | none (old site overflowed at 1440 home and 390 docs) |
| README quick start and Kafka example, verbatim | pass on `0.33.0` (2026-10-19) and `0.32.0` (2026-10-18) |
| README FpML example on the v0.33.0 binaries (x86_64 and aarch64 Linux in containers, Windows natively) | 14 rows each; the Windows output was checked for the README's three sample rows and contains all three |
| ICO frames | 16/32/48 read back; each identical to its separately rendered PNG |
| Mark reconstruction vs reference | 46/8,610 ink pixels differ by >50% coverage; colours exact |
| Rust/engine gates | not run on this branch: no Rust, Cargo or rustdoc-included file changed. Engine fixes are on separate branches (open issues). |

Performance (local static server, Chrome 155, cold cache, same machine): home 266.7 KB / 16
requests → 89.6 KB / 7 requests; docs 294.3 KB / 16 requests → 221.9 KB / 6 requests. Page timing
on localhost is not meaningful and engine performance is not implied.

## Open issues found during the audit

1. **Release pipeline:** the first 0.33.0 tag lacked the `v` prefix and produced no binaries,
   crate or Docker tag; `v0.33.0` replaced it on 2026-10-19. Every `Release` run since v0.27.0 is
   (including run 39200967980 for `v0.33.0`, whose assets, crate and image all published) is
   marked failed only because its last job, Update Compatibility Manifest, cannot push
   (`Permission to laminardb/laminardb.git denied to sujitn`, HTTP 403), so `compatibility.json`
   still says 0.18.10. Needs an owner to fix the `GH_PAT` secret, switch the job to
   `GITHUB_TOKEN`, or remove the job; a search found no reader of the file in this repository or
   the Python, Node.js and Java client repositories.
2. **Helm:** the Pages build set the chart version from `Cargo.toml` on every push to `main`, so
   chart 0.33.0 pointed at an image a day before it existed, earlier chart versions disappeared
   from the index, and chart 0.33.0 was republished with a new digest on 2026-10-19. Fixed on local
   branch `ci/helm-chart-release-version` (chart versioned and packaged from the release tag;
   published versions kept unchanged; home page laminardb.io). The chart's `laminardb.delivery`
   default (`best_effort`) still differs from the server's (`at_least_once`): a product decision.
3. **Files source (v0.32.0 and v0.33.0, run against both images):** a JSON file must hold exactly
   one object (JSON Lines fail with "trailing characters at line 2 column 1"); a failing file is
   retried continuously (about 180 warnings a second here) and blocks later files; fixing or
   deleting it does not clear it while the server runs (the source reports that the file changed
   size or is missing), and only a restart recovers; the CSV reader rejects `TIMESTAMP` and other
   narrower column types at startup. JSON Lines are fixed on `fix/files-json-lines` and CSV column
   types on `fix/csv-declared-column-types`; the stuck-file behaviour needs a decode-error policy
   and is not fixed.
4. **Server config (v0.33.0):** `[[lookup]]` without `strategy` defaults to `"poll"`, which the SQL
   parser rejects (the shipped `examples/laminardb.toml` sets it explicitly); fixed on
   `fix/lookup-default-strategy`. TOML watermark `max_out_of_orderness` is truncated to whole
   seconds; fixed on `fix/watermark-subsecond-delay`. `[[lookup]].cache.ttl` is still truncated to
   whole seconds.
5. **Stale docs not edited here:** `crates/laminar-server/README.md` still shows
   `cargo install laminar-server`, lacks `--pgwire-bind`, and its Kafka example omits
   `startup.mode` (fails under at-least-once); it and `SQL_REFERENCE.md` describe `psql`
   `SUBSCRIBE` that pgwire rejects; `SQL_REFERENCE.md` footer says DataFusion 52 / Arrow 57 /
   LaminarDB 0.22 and documents `slide(ts, size, slide)` while the parser treats `SLIDE` like `HOP`.
6. **Docs page:** PostgreSQL and MongoDB CDC sections were not re-audited; other sections were
   checked against code but not executed.
7. **Python bindings:** PyPI is at 0.17.0; release runs for later versions failed.
8. **Console:** the GHCR image is not anonymously pullable; at 390 px several scrolling table and
   chart wrappers are not keyboard-focusable (pre-existing); the hosted build will show the new
   branding only after `main` is deployed.
9. **Docs assistant:** the site's chat widget posts to an external worker whose answers were not
   evaluated; the widget now says answers can be wrong.
10. **Windows checkpoint paths (v0.33.0 binary):** with a 169-character `file:///` checkpoint
    directory the first checkpoint failed (`os error 3`) and the pipeline stopped (LDB-3024). The
    same pipeline worked from a 55-character directory, where the longest checkpoint file path
    was 208 characters, so the failure fits the 260-character Windows path limit. The docs
    page's `[checkpoint]` url row now says to keep the path short; the engine is not fixed.
