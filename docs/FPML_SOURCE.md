# FpML source

`format = 'fpml'` turns complete FpML documents into typed Arrow rows for streaming SQL. Each
document is validated against a pinned FpML schema closure, its references are checked, and a
projection profile maps it to columns: one row per trade, per document, or per loan event or
facility, depending on the profile. The files source and the Kafka source both accept the
format.

The format is behind the `fpml` Cargo feature (off by default). It builds a vendored, reduced
libxml2 2.15.4 with CMake; builds without the feature do not compile or link it.

## Support matrix

| Area | Supported | Not supported (rejected before I/O) |
|---|---|---|
| Standard | FpML 5.13 Recommendation 2, build 8, confirmation view (bundled, pinned by SHA-256); operator closures that extend it | Other releases or views without an operator closure; `xsi:schemaLocation` hints (ignored) |
| Transports | Files source (one document per file); Kafka source (one document per message, document-row profiles only) | Files and Kafka sinks; other transports |
| Coverage | All 50 products, 54 loan facilities and events, and 90 document roots in the closure, typed by bundled profiles ([support matrix](FPML_SUPPORT_MATRIX.md)) | |
| Deployment | Embedded and single-node server | Cluster mode: files sources are singletons, and a Kafka FpML source refuses a cluster vnode assignment |
| Delivery | At-least-once with checkpointing; best-effort Kafka may skip rejected messages | No new exactly-once path: the existing source and sink capability gates decide, and FpML is not separately qualified for exactly-once |
| Failure handling | A rejected document stops a guaranteed source at that document | Dead-letter or quarantine routing |

The evidence is the official FpML example corpus for 5.13 (535 schema-valid examples), plus
LaminarDB-authored synthetic fixtures for the representations the corpus does not exercise. These
are test evidence, not venue or counterparty certification: qualify each counterparty's documents
before production.

## How it works

1. **Bound the input.** A document is read only up to `fpml.max.document.bytes` (16 MiB by default;
   the files source also caps `max_file_bytes` to it, Kafka bounds messages with `record.max.bytes`).
2. **Pre-scan.** A streaming scan checks UTF-8 (with or without BOM, or an explicit UTF-8
   declaration) and rejects other encodings, DTDs, processing instructions, unknown entities, and
   documents over the depth, element, attribute, text, and namespace limits, before any tree is
   built. Nothing is decoded lossily.
3. **Validate.** libxml2 parses the document with network access, external entities, XInclude,
   and system catalogs disabled, then validates it against the pinned closure. Schema resources
   are served only from the closure's in-memory files; no path or URL in a document or schema can
   make it read anything else. Schema attribute defaults are applied.
4. **Check the root and references.** The root must be a document element of the pinned view's
   namespace, and `fpmlVersion` must equal the closure's release. Every `id` is unique and every
   `href` resolves (`fpml.references = 'resolve'`, the default); `fpml.references = 'schema'` also
   requires each target to have the type the schema's reference annotation names.
5. **Project.** The profile selects the row elements and fills each column from its selector.
   Values convert exactly (decimals, dates, date-times) or the document is rejected. A document
   is projected whole before any of its rows are returned: an invalid document yields no rows.
6. **Emit.** Rows carry the document SHA-256 and their locator; the source positions them so a
   restart resumes exactly (see [Delivery and recovery](#delivery-and-recovery)).

Validation and projection run on one dedicated worker thread per source. The async runtime only
hands documents to it, from a tracked blocking task.

## Profiles

A profile is a TOML file that names the closure it is written for, which elements become rows,
and the columns. Bundled profiles:

| Profile | One row per | Columns |
|---|---|---|
| `fpml-5-13-confirmation-trade` (files default) | trade of any asset class | Document and message context, trade identity and header, product type, parties, documentation; no product economics |
| `fpml-5-13-confirmation-rates` | interest rate trade | Trade columns plus `swap`, `fra`, `cap_floor`, `swaption`, `bullet_payment`, `bond_option` |
| `fpml-5-13-confirmation-fx` | FX trade or term deposit | Trade columns plus the 13 FX products and `term_deposit` |
| `fpml-5-13-confirmation-credit` | credit derivative trade | Trade columns plus `credit_default_swap`, `credit_default_swap_option` |
| `fpml-5-13-confirmation-equity-options` | equity option, forward, or accumulator trade | Trade columns plus 5 equity option products |
| `fpml-5-13-confirmation-equity-swaps` | total return or dividend swap trade | Trade columns plus 4 equity swap products |
| `fpml-5-13-confirmation-equity-volatility` | variance, volatility, or correlation swap trade | Trade columns plus 6 volatility products |
| `fpml-5-13-confirmation-commodity` | commodity trade | Trade columns plus 7 commodity products |
| `fpml-5-13-confirmation-securities` | repo, securities lending, or instrument trade | Trade columns plus `repo`, `security_lending`, `instrument_trade_details` |
| `fpml-5-13-confirmation-cross-asset` | generic, standard, or strategy trade | Trade columns plus `generic_product`, `standard_product`, `strategy` |
| `fpml-5-13-confirmation-document` (Kafka default) | document | Message envelope and every root-level element, with trades and events summarized |
| `fpml-5-13-confirmation-loan-servicing` | facility, loan contract, or letter of credit servicing event | Notification context plus the event |
| `fpml-5-13-confirmation-loan-trading` | loan allocation or loan trade event | Notification context plus the event |
| `fpml-5-13-confirmation-loan-facilities` | loan facility | Notification or statement context plus the facility |

Trade rows are every `trade` with a `tradeHeader`, wherever it appears: under the root, in a
trade package, or inside a business event such as an amendment or a clearing result. An
asset-class profile's rows are the trades whose product is in its class, so a document can yield
rows in several profiles; a trade is a row of exactly one asset-class profile. A strategy row
identifies its components; their economics are typed when traded on their own.

Every column carries a description in its Arrow field metadata. The trade profiles' column list
and nested structure are in `crates/laminar-connectors/fpml/profiles/fpml-5-13/confirmation/`.
Business messages are append-only facts: amendments, terminations, and corrections arrive as new
rows (`is_correction`, `trade_context`, `events`); the source never derives a current trade state.

## Configuration

Files, one document per file:

```sql
CREATE SOURCE rates FROM FILES (
    'path' = '/data/fpml/confirmations',
    'glob_pattern' = '*.xml',
    'fpml.profile' = 'fpml-5-13-confirmation-rates'
) FORMAT FPML;
```

Kafka, one document per message:

```sql
CREATE SOURCE messages (
    WATERMARK FOR creation_time_utc AS creation_time_utc - INTERVAL '5' SECOND
) FROM KAFKA (
    'bootstrap.servers' = 'broker:9092',
    'group.id' = 'fpml-ingest',
    'topic' = 'fpml.confirmations',
    'fpml.profile' = 'fpml-5-13-confirmation-document'
) FORMAT FPML;
```

The schema comes from the profile; declare no columns other than a watermark. Options:

| Option | Default | Meaning |
|---|---|---|
| `fpml.profile` | trade (files), document (Kafka) | Bundled profile name |
| `fpml.profile.path` | | Operator profile file instead of a bundled one |
| `fpml.schema.path` | bundled closure | Directory of an operator closure (`closure.toml`) |
| `fpml.references` | `resolve` | `resolve` or `schema` (also checks annotated target types) |
| `fpml.max.document.bytes` | 16 MiB (ceiling 256 MiB) | Largest document |
| `fpml.max.depth` | 64 | Element nesting |
| `fpml.max.elements` | 250,000 | Elements per document |
| `fpml.max.attributes.per.element` | 32 | |
| `fpml.max.text.bytes` | 1 MiB | One text node or attribute value |
| `fpml.max.namespace.declarations` | 64 | |
| `fpml.max.ids`, `fpml.max.references` | 100,000 each | `id` attributes and `href` references |
| `fpml.max.rows` | 10,000 | Rows from one document |
| `fpml.max.staged.bytes` | 64 MiB | Projected bytes held for one document |

A document over a limit is rejected; the limit is not a truncation point.

### Types

| Profile type | Arrow | Rule |
|---|---|---|
| `decimal`, `decimal(p, s)` | `Decimal128(38, 18)` or `(p, s)` | Exact; a value with more fractional digits than `s` is rejected, never rounded |
| `date32` | `Date32` | The written calendar day; a time-zone suffix is validated but not applied |
| `datetime` | `struct<utc, local, offset_minutes>` | `local` is the written wall time; `utc` and `offset_minutes` are null when no offset was written |
| `timestamp(unit, UTC)` | `Timestamp(unit, "+00:00")` | Requires an offset unless `offset = "optional"` (then null); a value finer than `unit` is rejected |
| `utf8`, `bool`, integers | as named | `whitespace = "collapse"` for normalized strings |
| `struct<template>`, `list<T>` | `Struct`, `List` | A scalar selector that matches two elements rejects the document; declare a list |

`creation_time_utc` (trade, document, and loan profiles) is the message creation instant for
event-time windows. It is null for data documents without a header and for headers written
without an offset: no UTC instant is invented. Nested fields are ordinary SQL:
`swap['streams'][1]['payer_party_href']`, `trade_header['trade_date']`.

## Extending

New products, operator extensions, or another closure need configuration only:

- **Closure.** A directory with `closure.toml` and the extension schemas. `base =
  "fpml-5-13-8-rec-2/confirmation"` adds the files to the bundled closure; every file is pinned by
  SHA-256 and resolved only inside the closure. `extension_namespaces` lists the namespaces whose
  elements carry `id`/`href` semantics.
- **Profile.** A root file with a `[profile]` header naming that closure, optional `include`s
  (relative files or `bundled:<path>` to reuse bundled templates and columns), `[namespaces]`,
  `[sets]`, `[structs.<name>]` templates, and `[[columns]]`.

The runnable example in `crates/laminar-connectors/tests/fixtures/fpml/extension/` adds a weather
index swap in its own namespace to the FpML `product` substitution group and maps it, reusing the
bundled trade columns:

```toml
include = ["bundled:fpml-5-13/confirmation/trade-columns.toml"]

[profile]
name = "example-weather-trades"
version = 1
artifact = "example-fpml-5-13-weather-1"
roots = "documents"
rows = "**/trade[tradeHeader][wx:weatherIndexSwap]"

[namespaces]
wx = "urn:example:fpml-weather"

[structs.weather_index_swap]
fields = [
  { name = "payer", type = "utf8", path = "wx:payerPartyReference->partyName" },
  { name = "strike", type = "decimal(18, 4)", path = "wx:strike" },
  { name = "notional", type = "struct<money>", path = "wx:notional" },
]

[[columns]]
name = "weather_index_swap"
type = "struct<weather_index_swap>"
path = "wx:weatherIndexSwap"
```

Selectors: `a/b` children, `**` descendants (rows only), `..` parent, `*` any child, `{set}` any
member of a set (a list or a substitution group), `x[name]` or `x[{set}]` elements with such a
child, `self::name` the current element only if it has that name, `->` follows the `href` to its
target, and `@attr`, `#name`, `#type`, `#locator` read values; `a | b` concatenates. A profile is
compiled against the closure's content models before the source starts: an unknown element,
a template that matches nothing, a template applied to an element of the wrong type, or an
incompatible type fails configuration, never ingestion.

## Delivery and recovery

- **Files.** The source checkpoints the processed-file inventory and, inside a file, the number
  of rows already emitted. A restart skips a document's committed rows and emits the rest; a file
  is never re-read past its committed rows.
- **Kafka.** One message is one document and one row, positioned by topic, partition, and offset.
  Only a profile whose rows selector is the document root (`rows = "."`) is admitted.
- **Contract.** The source records the closure fingerprint, release, build, view, profile name,
  version, and fingerprint in its schema binding when it is created. A restart with any of them
  changed fails before reading; drop and recreate the source to change them.
- **Guarantees.** At-least-once with checkpointing: rows may repeat after recovery when the sink is
  not checkpoint-committable. Exactly-once is admitted only where the existing capability gates
  admit the source and sink (a Kafka source into a certified sink, for example); this format adds
  no new path, and only at-least-once recovery is tested with FpML.

| Failure | Outcome |
|---|---|
| Malformed, oversized, wrongly encoded, schema-invalid, or reference-invalid document | Guaranteed delivery: the source stops at that document without advancing; best-effort Kafka: the message is skipped within `max.deser.error.rate` |
| Projection error (inexact decimal, ambiguous scalar, wrongly typed reference target) | As above; no partial rows from that document |
| Process killed | Restart resumes from the committed position; uncommitted rows are emitted again |
| Profile, closure, or release changed | Start fails with a schema mismatch |
| Validator thread lost | The decode fails and the source stops |

A stopped source needs an operator. Files: fix or remove the document, then restart. Kafka: a
guaranteed source cannot skip the message; recreate the source with `startup.mode = 'latest'` or a
consumer group whose committed offset is past the message (both skip data, so reconcile what was
skipped from the topic), or use best-effort delivery where losing rejected messages is
acceptable. There is no quarantine.

Recovery evidence: the standalone server ingested 45,000 documents arriving at 50 per second for
15 minutes while it was hard-killed every 15 seconds; every expected row reached a files sink,
with 177 at-least-once duplicates and nothing the documents do not contain
(`crates/laminar-server/tests/fpml_files_soak.rs`).

## Security and data handling

- No DTDs, external entities, XInclude, network or catalog access, XSLT, or schema location hints;
  schema resources come only from the pinned closure. Content under skip wildcards (an
  acknowledgement's `originalMessage`, MathML) is kept as data and never treated as references.
- Rejection details name the stage and line, not document content. Schema diagnostics are redacted
  to remove values. Documents never go to online validators.
- `document_sha256` is an integrity digest, not sender authentication. `ds:Signature` elements are
  schema-validated but not verified.
- Raw documents, rejected inputs, and error details are sensitive trade data: restrict the input
  paths and topics, and keep them out of general logs.

## Metrics

Each carries the `source` label; rejections also carry `stage`
(`prescan`, `parse`, `schema`, `root`, `references`, `projection`, `worker`).

| Metric | Meaning |
|---|---|
| `fpml_documents_accepted_total` | Documents that produced rows (or none) |
| `fpml_documents_rejected_total` | Rejected documents by stage |
| `fpml_rows_total` | Rows projected |
| `fpml_document_bytes_total` | Bytes of accepted documents |
| `fpml_decode_seconds` | Validation and projection time per document |

## Performance

Measured 2026-10-15 in `--release` on one desktop (AMD Ryzen 9 7900X, Windows 11), one source,
documents from the official corpus decoded end to end through the decoder
(`cargo bench -p laminar-connectors --features fpml --bench fpml_decoder`). These are local
numbers, not a target: rerun on production hardware before sizing.

| Case | Bytes | Rows | p50 | p99 | p99.9 | Docs/s | Rows/s |
|---|---|---|---|---|---|---|---|
| Interest rate swap, rates profile | 7,794 | 1 | 1.01 ms | 2.33 ms | 3.17 ms | 910 | 910 |
| Same swap, trade profile | 7,794 | 1 | 0.36 ms | 0.47 ms | 0.53 ms | 3,039 | 3,039 |
| FX spot, FX profile | 3,329 | 1 | 0.93 ms | 1.43 ms | 2.45 ms | 1,014 | 1,014 |
| CDS, credit profile | 7,511 | 1 | 0.58 ms | 0.92 ms | 1.19 ms | 1,640 | 1,640 |
| Commodity gas swap | 5,802 | 1 | 1.38 ms | 2.21 ms | 4.43 ms | 698 | 698 |
| Clearing message, document profile | 9,937 | 1 | 0.47 ms | 0.82 ms | 1.05 ms | 1,994 | 1,994 |
| Loan bulk servicing notice | 19,126 | 4 | 0.65 ms | 1.01 ms | 1.17 ms | 1,486 | 5,944 |
| 200 FX spot trades in one document | 276,630 | 200 | 15.7 ms | 24.8 ms | 26.8 ms | 61 | 12,112 |

Cost per document grows with the width of the profile's schema, not only with document size: the
asset-class profiles carry thousands of nested columns, so choose the narrowest profile that has
the columns you query.

## Operations

- **Changing a profile or closure.** Drop and recreate the source; the new source starts from the
  transport's start position or the oldest retained input.
- **Large documents.** Raise `fpml.max.document.bytes` and the transport's own bound together, and
  `fpml.max.rows` for documents with many trades.
- **Disk and topic access.** Restrict them to the LaminarDB service account; rejected documents
  stay in place for review.

## Licence notice

The bundled schemas and example documents are FpML Specifications under the FpML Public License
(Licensor: International Swaps and Derivatives Association, Inc.), see
`crates/laminar-connectors/fpml/artifacts/fpml-5-13-8-rec-2/NOTICE.md`; `xmldsig-core-schema.xsd`
is under the W3C Software License. libxml2 is under the MIT License, see
`vendor/libxml2-ffi/VENDORED.md`.
