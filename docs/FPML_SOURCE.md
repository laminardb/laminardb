# FpML source

FpML is the XML standard that most derivatives desks use for trade confirmations, clearing
messages, and loan notices. A single confirmation can hold hundreds of nested elements, references
that tie legs to parties and dates to schedules, and amounts that must never be rounded. Teams
usually end up writing a parser for each product they care about and maintaining it every time
the standard or a counterparty changes.

LaminarDB reads FpML documents directly. Set the format of a files, Kafka, RabbitMQ, or IBM MQ
source to `FPML`, or name the FIX field that carries the document, and each document is checked
against the official FpML schema, its internal references are resolved, and the parts you care
about become typed columns you can query with SQL. A profile decides what a
row is: one per trade, one per message, or one per loan event or facility.

## Before you start

FpML support is an optional build feature called `fpml`, and it's off by default. Turn it on when
you build the server:

```bash
cargo build --release -p laminar-server --features fpml
```

The feature compiles a trimmed copy of libxml2 2.15.4 with CMake, so you need CMake and a C
compiler. Builds without the feature don't compile or link libxml2.

## What's supported

| Area | Supported | Rejected before the source starts |
|---|---|---|
| Standard | FpML 5.13 Recommendation 2, build 8, confirmation view. It ships with LaminarDB and every schema file is pinned by SHA-256. You can extend it with your own schemas. | Other releases or views unless you supply their schemas. `xsi:schemaLocation` hints in documents are ignored. |
| Transports | Files source (one document per file); Kafka, RabbitMQ, and IBM MQ sources (one document per message, with a profile that gives one row per document); and a FIX source field such as SecurityXML (1185), which adds one `fpml` column per message | Sinks, and other transports |
| Coverage | Every product (50), loan facility and event (54), and document type (90) in the schema, each typed by a bundled profile. See the [support matrix](FPML_SUPPORT_MATRIX.md). | |
| Deployment | Embedded and single-node server | Cluster mode. A files source can't be spread across nodes, a Kafka FpML source refuses a cluster partition assignment, and cluster mode rejects RabbitMQ, IBM MQ, and FIX sources. |
| Delivery | At-least-once with checkpointing. A best-effort Kafka source can skip a rejected message. | No new exactly-once path. The usual source and sink rules decide, and FpML hasn't been tested on its own for exactly-once, including on a FIX source. |
| Bad documents | A rejected document stops a guaranteed source at that document. RabbitMQ and IBM MQ sources can route it through the broker instead, with their `on.decode.error` setting. | Files, Kafka, and FIX sources have no dead-letter or quarantine route |

The bundled profiles are tested against all 535 schema-valid example documents published with
FpML 5.13, plus synthetic documents written by the LaminarDB team for the cases the official
examples don't cover. That's test evidence, not venue or counterparty certification. Before you go
live, run a sample of each counterparty's real documents through a test source.

## Reading documents

### From files

Each `.xml` file is one document:

```sql
CREATE SOURCE rates FROM FILES (
    'path' = '/data/fpml/confirmations',
    'glob_pattern' = '*.xml',
    'fpml.profile' = 'fpml-5-13-confirmation-rates'
) FORMAT FPML;
```

### From Kafka

Each message is one document, and each document becomes one row, so Kafka sources take a profile
whose rows are whole documents. The default is the document profile:

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

You don't declare columns. The profile defines them, and the only thing you can add is a
watermark. Nested values work like any other SQL struct or list, for example
`trade_header['trade_date']` or `swap['streams'][1]['payer_party_href']`.

The [README](../README.md#fpml-trade-confirmations) has a walkthrough that runs on your machine in
a few minutes.

### From RabbitMQ or IBM MQ

Queues work like Kafka: each message is one document and one row, the document profile is the
default, and you don't declare columns. Metadata columns are added after the profile's columns:

```sql
CREATE SOURCE confirmations FROM RABBITMQ (
    'url' = 'amqps://broker.example.com:5671/trading',
    'username' = 'laminar',
    'password' = '${RABBITMQ_PASSWORD}',
    'queue' = 'fpml.confirmations',
    'metadata.columns' = 'message_id,redelivered'
) FORMAT FPML;
```

`FROM IBMMQ (...) FORMAT FPML` takes the same `fpml.*` options. The rest of the
[RabbitMQ](RABBITMQ.md) and [IBM MQ](IBMMQ.md) settings apply unchanged.

### From a FIX field

FIX 5.0 SP1 and later carry an FpML document in SecurityXML (1185), with its length in
SecurityXMLLen (1184). Name the field with `fpml.field` and the source adds a column called `fpml`
after the columns you declare. It's a struct of the profile's columns, so you read it like any
other struct:

```sql
CREATE SOURCE confirmations (
    exec_id VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL
) FROM FIX (
    -- session, dictionary, and journal options as in the FIX source guide
    messages = '8',
    'column.exec_id' = '17',
    'column.symbol' = '55',
    'fpml.field' = '1185'
);

SELECT exec_id, fpml['document_root'], fpml['header']['message_id'] FROM confirmations;
```

- The field must be a body field of a selected message type, typed DATA or XMLDATA in your
  dictionary. A message without it has a null `fpml`.
- Each message holds one document, so the profile must give one row per document. The document
  profile is the default. If `expand` turns a message into several rows, each row carries the same
  document.
- The document must be a complete FpML document in the configured view. Fragments, or documents
  from another view or release, are rejected like any other bad document.
- `max.message.bytes` on the FIX source bounds the whole message, document included.

## Choosing a profile

A profile is a TOML file that says which elements become rows and what the columns are. LaminarDB
ships with fourteen:

| Profile | One row per | What you get |
|---|---|---|
| `fpml-5-13-confirmation-trade` (files default) | Trade of any asset class | Document and message context, trade identity and header, product type, parties, and documentation, without product economics |
| `fpml-5-13-confirmation-rates` | Interest rate trade | The trade columns plus `swap`, `fra`, `cap_floor`, `swaption`, `bullet_payment`, and `bond_option` |
| `fpml-5-13-confirmation-fx` | FX trade or term deposit | The trade columns plus the 13 FX products and `term_deposit` |
| `fpml-5-13-confirmation-credit` | Credit derivative trade | The trade columns plus `credit_default_swap` and `credit_default_swap_option` |
| `fpml-5-13-confirmation-equity-options` | Equity option, forward, or accumulator | The trade columns plus 5 equity option products |
| `fpml-5-13-confirmation-equity-swaps` | Total return or dividend swap | The trade columns plus 4 equity swap products |
| `fpml-5-13-confirmation-equity-volatility` | Variance, volatility, or correlation swap | The trade columns plus 6 volatility products |
| `fpml-5-13-confirmation-commodity` | Commodity trade | The trade columns plus 7 commodity products |
| `fpml-5-13-confirmation-securities` | Repo, securities lending, or instrument trade | The trade columns plus `repo`, `security_lending`, and `instrument_trade_details` |
| `fpml-5-13-confirmation-cross-asset` | Generic, standard, or strategy trade | The trade columns plus `generic_product`, `standard_product`, and `strategy` |
| `fpml-5-13-confirmation-document` (Kafka default) | Document | The message envelope and every top-level element, with trades and events summarized |
| `fpml-5-13-confirmation-loan-servicing` | Facility, loan contract, or letter of credit servicing event | The notification context plus the event |
| `fpml-5-13-confirmation-loan-trading` | Loan allocation or loan trade event | The notification context plus the event |
| `fpml-5-13-confirmation-loan-facilities` | Loan facility | The notification or statement context plus the facility |

A few things help when you pick one:

- **Pick the narrowest profile that has your columns.** The asset-class profiles carry thousands
  of nested columns, and that width, more than document size, decides how long each document
  takes. If you only need trade identity and dates, the trade profile is about three times faster
  than the rates profile on the same swap.
- **A trade is any `trade` with a `tradeHeader`, wherever it sits**: at the top of the document,
  in a trade package, or inside a business event such as an amendment or a clearing result.
- **Every trade belongs to exactly one asset-class profile**, so one document can feed several
  sources if it holds trades from different classes. A strategy row lists its components; their
  economics appear when they're traded on their own.
- **Messages are facts, not trade state.** Amendments, terminations, and corrections arrive as new
  rows (`is_correction`, `trade_context`, `events`). The source never works out a trade's current
  state for you; that's a job for your SQL.

Every column carries a description in its Arrow field metadata. To read the full column list and
nested structure, open the profiles in
`crates/laminar-connectors/fpml/profiles/fpml-5-13/confirmation/`.

## What happens to each document

1. **The size is checked.** A document is read only up to `fpml.max.document.bytes` (16 MiB by
   default). The files source caps `max_file_bytes` to the same value, Kafka bounds messages with
   `record.max.bytes`, and RabbitMQ, IBM MQ, and FIX bound them with `max.message.bytes`.
2. **A quick scan runs before any parsing.** Only UTF-8 is accepted, with or without a byte order
   mark or an explicit UTF-8 declaration. Other encodings are rejected rather than converted, so
   nothing is ever decoded lossily. The scan also rejects DTDs, processing instructions, unknown
   entities, and documents over the depth, element, attribute, text, or namespace limits.
3. **The document is validated against the schema.** libxml2 parses it with network access,
   external entities, XInclude, and system catalogs turned off, and validates it against the
   pinned schema files, applying the schema's default attribute values. Schema files are served
   only from memory, from the pinned set, so nothing in a document or schema can make LaminarDB
   read another file or URL.
4. **The root and references are checked.** The root must be a document element from the
   configured FpML view, and its `fpmlVersion` must match the schema's release. Every `id` must be
   unique and every `href` must point at something. With `fpml.references = 'schema'`, each target
   must also have the type the schema says it should.
5. **The profile turns it into rows.** Values are converted exactly or the document is rejected.
   The whole document is converted before any row is released, so a document either produces all
   its rows or none of them.
6. **Rows go downstream.** Each row carries the document's SHA-256 and a locator that says where it
   came from in the document. The source records its position so a restart picks up where it
   left off.

Validation and conversion run on a dedicated thread for each source, so slow documents never
block the async runtime.

## Options

| Option | Default | What it does |
|---|---|---|
| `fpml.profile` | trade for files, document for message sources and FIX fields | A bundled profile name |
| `fpml.profile.path` | | Your own profile file, instead of a bundled one |
| `fpml.schema.path` | The bundled schema | A directory with your own `closure.toml` and schema files |
| `fpml.references` | `resolve` | `resolve` checks that every `href` points at something; `schema` also checks the target's type |
| `fpml.max.document.bytes` | 16 MiB (at most 256 MiB) | The largest document |
| `fpml.max.depth` | 64 | How deeply elements can nest |
| `fpml.max.elements` | 250,000 | Elements in one document |
| `fpml.max.attributes.per.element` | 32 | Attributes on one element |
| `fpml.max.text.bytes` | 1 MiB | One text value or attribute value |
| `fpml.max.namespace.declarations` | 64 | Namespace declarations in one document |
| `fpml.max.ids`, `fpml.max.references` | 100,000 each | `id` attributes and `href` references |
| `fpml.max.rows` | 10,000 | Rows from one document |
| `fpml.max.staged.bytes` | 64 MiB | Converted data held for one document before its rows are released |

A document over any limit is rejected. Limits never cut a document short.

## Column types

| Profile type | Arrow type | How values are converted |
|---|---|---|
| `decimal`, `decimal(p, s)` | `Decimal128(38, 18)`, or `(p, s)` | Exactly. A value with more decimal places than `s` is rejected, never rounded. |
| `date32` | `Date32` | The calendar day as written. A time zone suffix is checked but not applied. |
| `datetime` | `struct<utc, local, offset_minutes>` | `local` is the wall-clock time as written. `utc` and `offset_minutes` are null when the document gave no offset. |
| `timestamp(unit, UTC)` | `Timestamp(unit, "+00:00")` | Needs an offset unless the profile sets `offset = "optional"`, in which case a missing offset gives null. A value more precise than `unit` is rejected. |
| `utf8`, `bool`, integers | As named | `whitespace = "collapse"` normalizes spacing in strings |
| `struct<template>`, `list<T>` | `Struct`, `List` | If a single-value column matches two elements, the document is rejected. Use a list for repeating elements. |

The trade, document, and loan profiles have a `creation_time_utc` column, the time the message
was created, which is the natural choice for event-time windows. It's null when a data document has
no header or the header's time was written without an offset. LaminarDB doesn't guess a time zone.

## Using your own schemas and columns

New products, in-house extensions, or a different FpML release don't need code changes. You need
two files: a schema bundle and a profile.

**The schema bundle** is a directory with a `closure.toml` and your schema files. To add to the
bundled schema, set `base = "fpml-5-13-8-rec-2/confirmation"`. Leave `base` out to replace the
schema entirely, for example with another release or view; you then need your own profile too,
because the bundled ones are written for 5.13 confirmations. Every file is pinned by SHA-256 and
can only be loaded from inside the directory. `extension_namespaces` lists the namespaces whose
elements use FpML's `id` and `href` attributes.

```toml
artifact = "example-fpml-5-13-weather-1"
base = "fpml-5-13-8-rec-2/confirmation"
release = "5-13"
view = "confirmation"
namespace = "http://www.fpml.org/FpML-5/confirmation"
extension_namespaces = ["urn:example:fpml-weather"]
entry = "weather-extension.xsd"

[files]
"weather-extension.xsd" = "454c83fb9ab75eb5e75c551b71601ae23932fac2152cf8851dfbce6c15bd5751"
```

**The profile** starts with a `[profile]` header that names the schema bundle. It can `include`
other files, either relative paths or `bundled:<path>` to reuse the bundled columns and templates,
and then declares `[namespaces]`, `[sets]`, `[structs.<name>]` templates, and `[[columns]]`.
This one adds a weather index swap and reuses every bundled trade column:

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

Point a source at them with `fpml.schema.path` and `fpml.profile.path`. The complete example,
including the schema and a sample document, is in
`crates/laminar-connectors/tests/fixtures/fpml/extension/`, and it runs as part of the test suite.

Paths in a profile use a small selector language:

| Selector | Meaning |
|---|---|
| `a/b` | Child `b` of child `a` |
| `**` | Any descendant (in the `rows` selector only) |
| `..` | The parent |
| `*` | Any child |
| `{set}` | Any member of a set, either a list you declare or a schema substitution group |
| `x[name]`, `x[{set}]` | Elements `x` that have such a child |
| `self::name` | The current element, only if it has that name |
| `->` | Follow the `href` to the element it points at |
| `@attr`, `#text`, `#name`, `#type`, `#locator` | An attribute, the element's text, its local name, its `xsi:type`, or where it sits in the document |
| `a \| b` | Both, concatenated |

LaminarDB checks a profile against the schema before the source starts. An unknown element, a
template that matches nothing, a template applied to the wrong element type, or an incompatible
column type is a configuration error when the source is created, not a rejected document later.

## Delivery, recovery, and failures

**Files.** The source checkpoints which files it has finished and, inside a file, how many rows it
has already sent. After a restart it skips the rows that were committed and sends the rest. It
never reads a file again past its committed rows.

**Kafka.** One message is one document and one row, positioned by topic, partition, and offset.
Only profiles whose rows are whole documents (`rows = "."`) are accepted.

**RabbitMQ and IBM MQ.** One message is one document and one row, with the same profile rule. A
message is acknowledged (RabbitMQ) or committed (IBM MQ) only when the checkpoint that holds its
row is durable, as for any other format. On IBM MQ, validation runs inside the get, so
`operation.timeout.ms` has to cover validating a full batch.

**FIX.** The document is validated when its message is read from the journal, live or on replay.
The field, schema bundle, and profile are part of the source's contract, so replaying the journal
gives the same `fpml` values.

**The contract is fixed when the source is created.** LaminarDB records the schema bundle's
fingerprint, release, build, and view, and the profile's name, version, and fingerprint. If any of
them changes, the next start fails with a schema mismatch before reading anything. To change
them, drop the source and create it again.

**Guarantees.** Delivery is at-least-once with checkpointing, so rows can repeat after a crash
unless the sink commits with the checkpoint. Exactly-once is available only where the usual rules
already allow it, for example a Kafka source into a certified sink. FpML doesn't add a new
exactly-once path, and only at-least-once recovery has been tested with FpML.

| What goes wrong | What happens |
|---|---|
| A document is malformed, too large, in the wrong encoding, invalid against the schema, or has a broken reference | A guaranteed source stops at that document without moving past it. A best-effort Kafka source skips the message, within `max.deser.error.rate`. A RabbitMQ or IBM MQ source with a dead-letter setting hands the message to the broker instead. |
| A value can't be converted exactly, a single-value column matches two elements, or a reference points at the wrong type | The same, and none of that document's rows are sent |
| The process is killed | The source resumes from its last committed position, and rows after it are sent again |
| The profile, schema, or release changed | The source refuses to start, with a schema mismatch |
| The validation thread dies | The current document fails and the source stops |

When a source stops on a bad document, someone has to decide what to do with it:

- **Files:** fix or remove the document, then restart.
- **Kafka:** a guaranteed source can't skip the message. Create the source again with
  `startup.mode = 'latest'`, or with a consumer group whose committed offset is past the message.
  Both skip data, so check the topic for what was skipped. If losing rejected messages is
  acceptable, use best-effort delivery instead.
- **RabbitMQ:** the message stays unacknowledged and comes back after recovery. To set rejected
  documents aside, give the queue a dead-letter exchange and set `on.decode.error = 'reject'`.
- **IBM MQ:** the get is backed out and the message comes back. To set rejected documents aside,
  give the queue `BOTHRESH` and a local `BOQNAME` and set `on.decode.error = 'backout-queue'`; the
  move commits with the checkpoint.
- **FIX:** the record stays in the journal, so the source stops at it again after every restart,
  as it does for a FIX value that can't be converted. There's no setting to skip one record.
  Dropping the source and creating it again without `fpml.field` reads the session without FpML.

We tested recovery by hard-killing the standalone server every 15 seconds for 15 minutes while
45,000 documents arrived at 50 per second. Every expected row reached the files sink. There were
177 at-least-once duplicates and no rows that the documents don't contain. The test is
`crates/laminar-server/tests/fpml_files_soak.rs`.

## Security and sensitive data

- LaminarDB never processes DTDs, external entities, XInclude, network or catalog lookups, XSLT, or
  schema location hints. Schema files come only from the pinned set. Content inside wildcard
  elements, such as an acknowledgement's `originalMessage` or embedded MathML, is kept as data and
  never treated as references.
- Rejection messages name the stage and line, not document content, and schema errors have their
  values removed. Documents are never sent to online validators.
- `document_sha256` is an integrity digest. It doesn't authenticate the sender. `ds:Signature`
  elements are checked against the schema, but the signature isn't verified.
- Treat raw documents, rejected documents, and error details as sensitive trade data. Restrict
  access to the input directories and topics, and keep them out of general logs.

## Monitoring

Every metric has a `source` label. Rejections also have a `stage` label: `prescan`, `parse`,
`schema`, `root`, `references`, `projection`, or `worker`.

| Metric | What it counts |
|---|---|
| `fpml_documents_accepted_total` | Documents that were accepted, whether or not they produced rows |
| `fpml_documents_rejected_total` | Rejected documents, by stage |
| `fpml_rows_total` | Rows produced |
| `fpml_document_bytes_total` | Bytes in accepted documents |
| `fpml_decode_seconds` | Validation and conversion time per document |

## Performance

These numbers come from one desktop (AMD Ryzen 9 7900X, Windows 11) on 2026-10-15, built with
`--release`, with one source decoding documents from the official examples
(`cargo bench -p laminar-connectors --features fpml --bench fpml_decoder`). Treat them as a rough
guide and measure on your own hardware before you size anything.

| Document | Bytes | Rows | p50 | p99 | p99.9 | Docs/s | Rows/s |
|---|---|---|---|---|---|---|---|
| Interest rate swap, rates profile | 7,794 | 1 | 1.01 ms | 2.33 ms | 3.17 ms | 910 | 910 |
| Same swap, trade profile | 7,794 | 1 | 0.36 ms | 0.47 ms | 0.53 ms | 3,039 | 3,039 |
| FX spot, FX profile | 3,329 | 1 | 0.93 ms | 1.43 ms | 2.45 ms | 1,014 | 1,014 |
| CDS, credit profile | 7,511 | 1 | 0.58 ms | 0.92 ms | 1.19 ms | 1,640 | 1,640 |
| Commodity gas swap | 5,802 | 1 | 1.38 ms | 2.21 ms | 4.43 ms | 698 | 698 |
| Clearing message, document profile | 9,937 | 1 | 0.47 ms | 0.82 ms | 1.05 ms | 1,994 | 1,994 |
| Loan bulk servicing notice | 19,126 | 4 | 0.65 ms | 1.01 ms | 1.17 ms | 1,486 | 5,944 |
| 200 FX spot trades in one document | 276,630 | 200 | 15.7 ms | 24.8 ms | 26.8 ms | 61 | 12,112 |

## Running it

- **Changing a profile or schema:** drop the source and create it again. The new source starts
  from the transport's start position or the oldest input still available.
- **Large documents:** raise `fpml.max.document.bytes` together with the transport's own limit,
  and raise `fpml.max.rows` for documents that hold many trades.
- **Access:** only the LaminarDB service account should be able to read the input directories and
  topics. Rejected documents stay where they are, so you can review them.

## Licences

The bundled schemas and example documents are FpML Specifications under the FpML Public License
(Licensor: International Swaps and Derivatives Association, Inc.); see
`crates/laminar-connectors/fpml/artifacts/fpml-5-13-8-rec-2/NOTICE.md`. `xmldsig-core-schema.xsd`
is under the W3C Software License. libxml2 is under the MIT License; see
`vendor/libxml2-ffi/VENDORED.md`.
