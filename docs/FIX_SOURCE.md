# FIX source

The `fix` source connects to a FIX counterparty as an initiator, captures selected application
messages durably, and emits them as Arrow rows for streaming SQL. The session protocol runs in
the QuickFIX C++ engine; LaminarDB owns the receive journal, projection, checkpoints, and replay.

The connector is behind the `fix` Cargo feature, which is off by default and builds the engine
with CMake and a C++17 compiler. Builds without the feature do not compile or link the engine.

## Support matrix

| Area | Supported | Not supported (rejected before I/O) |
|---|---|---|
| Role | Initiator, one session per source | Acceptor |
| Versions | FIX.4.4 (qualified with local tests), FIX.4.2 (same engine path, not separately exercised) | FIXT.1.1 / FIX 5.x, FIXP, FAST, SBE |
| Transport | TLS with chain and hostname verification, optional mutual TLS; plaintext TCP to loopback, or to a remote host only with `tcp.allow.remote` | Engine-native SSL |
| Deployment | Embedded, single-node server | Cluster (the journal and engine store are node-local) |
| Delivery | At-least-once with checkpointing | Best-effort, exactly-once |
| Outbound traffic | Session messages only (Logon, Heartbeat, ResendRequest, Logout, Reject) | Application messages, subscription requests |
| Host failure | `journal.sync = fsync` survives process, OS, and power failure of the host | Loss of the node or its disk |

Validation evidence is local: a real QuickFIX acceptor on loopback with a synthetic dictionary
(`crates/laminar-connectors/tests/fixtures/fix/FIX44-synthetic.xml`). It is not venue
certification. Qualify every venue with its own dictionary, schedule, and conformance tests.

## How it works

1. The engine validates each inbound message against the dictionary and checks CompIDs,
   SendingTime, and sequence number.
2. For a selected message type, the source writes the engine's serialization of the message to
   the journal inside the engine callback and returns only once the record is durable. Until
   then the engine cannot advance its inbound sequence number. If the write cannot complete, the
   session logs out without advancing, and the counterparty retransmits after reconnecting.
3. The source reads durable journal records, parses them with the pinned dictionary, and emits
   rows. Each row is positioned by `(session, journal index, row ordinal)`. A checkpoint stores
   the journal index; committed checkpoints release whole journal segments.
4. On restart the source replays the journal from the committed index without contacting the
   counterparty, and the session resumes from the engine store. Retransmissions of already
   journaled sequence numbers are dropped after a content comparison that ignores
   retransmission headers; a retransmission with different content stops the source.

The journal holds the engine's re-serialization (`Message::toString`), which recomputes
BodyLength and CheckSum. It is not guaranteed to equal the bytes on the wire.

## Configuration

```sql
CREATE SOURCE fills (
    exec_id        VARCHAR NOT NULL,
    order_id       VARCHAR NOT NULL,
    symbol         VARCHAR NOT NULL,
    last_qty       DECIMAL(18, 4),
    last_px        DECIMAL(18, 8),
    transact_time  TIMESTAMP NOT NULL,
    _fix_seq_num   BIGINT NOT NULL,
    _fix_series    BIGINT NOT NULL,
    _fix_poss_dup  BOOLEAN NOT NULL,
    WATERMARK FOR transact_time AS transact_time - INTERVAL '2' SECOND
) FROM FIX (
    host = 'fix.venue.example',
    port = '9443',
    'tls.ca.file' = '/etc/laminardb/fix/venue-ca.pem',
    'sender.comp.id' = 'LAMINAR',
    'target.comp.id' = 'VENUE',
    username = 'laminar',
    password = '${FIX_PASSWORD}',
    'dictionary.path' = '/etc/laminardb/fix/venue-FIX44.xml',
    messages = '8',
    'journal.path' = '/var/lib/laminardb/fix/venue',
    'schedule.start.time' = '06:00:00',
    'schedule.end.time' = '22:00:00',
    'column.exec_id' = '17',
    'column.order_id' = '37',
    'column.symbol' = '55',
    'column.last_qty' = '32',
    'column.last_px' = '31',
    'column.transact_time' = '60'
);
```

Pass secrets as environment references (`${VAR}`); durable DDL rejects literal secrets. The
password is sent in Logon (554) and never written by the source: outbound messages are not
persisted (`PersistMessages=N`), engine message logging is disabled, and journal records hold
inbound application messages only.

| Option | Default | Meaning |
|---|---|---|
| `host`, `port` | required | Counterparty endpoint |
| `transport` | `tls` | `tls` or `tcp` |
| `tls.ca.file` | public web roots | PEM CA bundle for the counterparty |
| `tls.server.name` | `host` | Name verified against the server certificate |
| `tls.client.cert.file`, `tls.client.key.file` | none | PEM client identity for mutual TLS |
| `tcp.allow.remote` | `false` | Permit plaintext to a non-loopback host |
| `begin.string` | `FIX.4.4` | `FIX.4.4` or `FIX.4.2` |
| `sender.comp.id`, `target.comp.id` | required | Session CompIDs |
| `session.qualifier` | none | Engine session qualifier |
| `username`, `password` | none | Logon Username (553) / Password (554) |
| `dictionary.path` | required | Dictionary XML, at most 16 MiB; pinned by SHA-256 at CREATE SOURCE |
| `messages` | required | Comma-separated application MsgTypes to capture |
| `journal.path` | required | Journal, engine store, and pinned dictionary directory; exclusively locked |
| `journal.sync` | `fsync` | `fsync` (host failure) or `os` (process failure only) |
| `journal.max.bytes` | 1 GiB | Retained bytes above which intake pauses |
| `journal.segment.bytes` | 64 MiB | Segment size; retention deletes whole segments |
| `max.message.bytes` | 1 MiB | Largest captured message; larger ones stop the source |
| `max.rows.per.message` | 10000 | Bound on expanded rows and list entries per message |
| `heartbeat.interval.seconds` | 30 | HeartBtInt (108) |
| `reconnect.interval.seconds` | 5 | Fixed engine reconnect interval |
| `logon.timeout.seconds`, `logout.timeout.seconds` | 10, 2 | Engine timeouts |
| `schedule.start.time`, `schedule.end.time` | `00:00:00` | Session window; equal values mean 24 hours |
| `schedule.start.day`, `schedule.end.day` | none | Weekly session (both or neither) |
| `schedule.time.zone` | `utc` | `utc` or `local` (the engine supports no other zones) |
| `reset.on.logon`, `reset.on.logout`, `reset.on.disconnect` | `false` | Sequence-number reset policy agreed with the venue |
| `validate.user.defined.fields` | `true` | Validate tags 5000 and above |
| `validate.fields.out.of.order` | `true` | Reject header/body/trailer ordering errors |
| `validate.fields.have.values` | `true` | Reject empty fields |
| `allow.unknown.message.fields` | `false` | Accept fields the dictionary does not define for a message |
| `check.latency`, `max.latency.seconds` | `true`, 120 | SendingTime accuracy check |
| `on.sequence.reset` | `fail` | `fail` or `continue` after an unrecoverable SequenceReset-Reset |
| `timestamp.precision` | `exact` | `exact` fails on digits finer than the column unit; `truncate` drops them |
| `expand` | none | Group count tag whose entries become rows |
| `expand.empty` | `row` | `row` (one row with null entry values) or `skip` |
| `column.<name>` | required per column | Field path; see below |

Unknown options are rejected, as are engine settings outside this list. The source's Logon
carries MaxMessageSize (383) = `max.message.bytes`, because the engine buffers a whole message
before any local limit applies.

## Column mapping

Every declared column needs a mapping, except reserved `_fix_*` metadata columns.

| Path | Reads |
|---|---|
| `TAG` | Body field |
| `header.TAG`, `trailer.TAG` | Header or trailer field |
| `GROUP.TAG` | One member of every entry of a body group, as `ARRAY<T>` in entry order (a missing member is a NULL item) |
| `GROUP.NESTED.TAG` | A member of a nested group, as `ARRAY<ARRAY<T>>`, one inner array per outer entry |
| `GROUP` | The whole group as `List<Struct>`, with struct fields mapped by `column.<name>.<field>`; struct columns can be declared through the Arrow API but not in SQL DDL, which has no `STRUCT` type |
| `GROUP[].TAG`, `GROUP[].NESTED.TAG` | Field, or nested-group member list, of the current `expand` entry |
| `@metadata` | Connector metadata |

Mappings are checked against the declared types and the dictionary at CREATE SOURCE: unknown
tags, fields that no selected message defines, wrong sections, and incompatible types fail.

| Arrow type | FIX field types | Conversion |
|---|---|---|
| `VARCHAR` | all except DATA | Exact text, leading zeros kept; invalid UTF-8 fails |
| `BYTEA` | all | Exact bytes; DATA fields keep embedded SOH and NUL |
| `BIGINT`, `INT`, unsigned | INT, SEQNUM, LENGTH, NUMINGROUP, ... | Exact integer; out of range fails |
| `DECIMAL(p, s)` | PRICE, QTY, AMT, FLOAT, INT, ... | Exact; more fractional digits than `s` or more than `p` digits fails |
| `TIMESTAMP` (any unit) | UTCTIMESTAMP | Exact; sub-unit digits fail unless `timestamp.precision = truncate` |
| `DATE` | LOCALMKTDATE, UTCDATEONLY | Days since epoch |
| `BOOLEAN` | BOOLEAN | `Y` / `N` |

SQL `TIMESTAMP` columns are microseconds. A venue that sends nanosecond TransactTime needs
`timestamp.precision = 'truncate'` (or a VARCHAR column); with `exact` the first such value stops
the source.

A missing field is NULL; a missing field in a `NOT NULL` column, or an invalid value, stops the
source at that journal record with the session, series, sequence number, and column. Values are
never coerced to NULL. An absent group is a NULL list; a group present with zero entries is an
empty list. Member arrays of one group have the same length and order, so position `i` in each
describes entry `i`. Expanding one group never forms a product with other groups.

| Metadata (`@name` or column `_fix_name`) | Type | Meaning |
|---|---|---|
| `msg_type` | VARCHAR | MsgType (35) |
| `seq_num` | BIGINT / unsigned | MsgSeqNum (34) |
| `series` | integer | Sequence series; increments on a genuine reset |
| `row` | integer | Row ordinal within the message (expanded entry) |
| `poss_dup`, `poss_resend` | BOOLEAN | PossDupFlag (43), PossResend (97) as received |
| `received_at` | TIMESTAMP | Wall-clock capture time, journaled; replay keeps the original |
| `journal_index` | BIGINT / unsigned | Journal record index |
| `session` | VARCHAR | `BeginString:Sender->Target` |
| `message` | BYTEA | Engine serialization of the message (not wire-exact) |
| `gap_before` | BOOLEAN | First row after an unrecoverable SequenceReset-Reset |

Event identity is `(session, series, seq_num, row)`. A reconnect or restart never changes it.
Mixed message types share one schema: select several MsgTypes and declare columns that some types
do not carry as nullable.

### Custom fields and groups

Venue dialects are configuration: put custom fields, messages, and groups in the dictionary, then
map them. The synthetic dictionary defines `VenueSeq` (5001), `VenueTag` (5002), and message `U1`
with `VenueStatusCode` (5003):

```sql
CREATE SOURCE venue_events (
    order_id      VARCHAR,
    venue_seq     BIGINT,
    status        BIGINT,
    party_id      VARCHAR,
    party_sub_ids ARRAY<VARCHAR>,
    fee_amounts   ARRAY<DECIMAL(18, 2)>,
    fee_currency  ARRAY<VARCHAR>,
    _fix_msg_type VARCHAR NOT NULL,
    _fix_row      BIGINT NOT NULL
) FROM FIX (
    ...,
    messages = '8,U1',
    expand = '453',
    'column.order_id' = '37',
    'column.venue_seq' = '5001',
    'column.status' = '5003',
    'column.party_id' = '453[].448',
    'column.party_sub_ids' = '453[].802.523',
    'column.fee_amounts' = '136.137',
    'column.fee_currency' = '136.138'
);
```

Each party (453) becomes a row; its sub-IDs stay inside the row; the fee group (136) is carried
whole on every row of the message as two aligned arrays.

## Delivery and recovery contract

The source declares a commit-coupled, singleton, append-only contract with deterministic row
positions, so it is admitted only with `AtLeastOnce` delivery and checkpointing, outside cluster
mode. Rows may repeat after recovery when a sink is not checkpoint-committable.

| Failure | Outcome |
|---|---|
| Process killed at any point | No accepted message lost; the session resumes from the engine store and the counterparty retransmits anything not journaled; retransmissions are deduplicated |
| Host or power failure, `journal.sync = fsync` | As above for inbound data. The outbound sequence number may regress (the engine store does not fsync); the next logon may then be rejected as "MsgSeqNum too low" and needs operator reconciliation |
| Host or power failure, `journal.sync = os` | Not covered: journal records may be lost; a restore past the journal end fails |
| Node or disk loss | Not covered: the journal and engine store are on that node |
| Journal write or fsync failure | The writer stops (no retry after an ambiguous fsync); the session logs out without advancing; the source fails; restart truncates a torn tail |
| Journal full | Intake pauses (session logs out and refuses logon) until committed checkpoints release segments |
| Corrupt record before the durable end | The source fails; no fallback |
| Restore point older than the retained journal | The source fails with "restore point expired" |

Changing the session identity, dictionary content, message selection, or validation flags
changes the journal's capture contract; a journal with a different contract is rejected. Change
the column mapping by dropping and recreating the source: the pipeline identity changes, the new
source starts from the oldest retained journal record, and the session continues.

## Sequence numbers and completeness

- The engine requests resends for gaps. SequenceReset-GapFill moves past administrative or
  intentionally skipped numbers and is not loss; missing numbers in the emitted rows are expected
  because administrative and unselected messages share the sequence space.
- SequenceReset-Reset that skips numbers is unrecoverable history. With `on.sequence.reset = fail`
  the source stops at that journal record; reconcile with the venue, then set `continue` to emit
  rows after it with `gap_before = true`. `fix_source_feed_incomplete` stays 1 for the run.
- A series starts when an accepted Logon carries ResetSeqNumFlag=Y or the engine store was reset
  (configured reset or a new schedule period).
- A counterparty that renumbers without ResetSeqNumFlag violates the protocol; the engine logs
  out with "MsgSeqNum too low", and the source does not reset automatically.

## Metrics

All metrics carry only the `source` label.

| Metric | Meaning |
|---|---|
| `fix_source_logged_on` | 1 while logged on |
| `fix_source_next_target_seq` | Next expected inbound sequence number after the last captured message |
| `fix_source_journal_retained_bytes`, `fix_source_journal_unread_records` | Journal size and projection lag |
| `fix_source_checkpoint_lag_records` | Durable journal records after the last committed checkpoint |
| `fix_source_startup_replay_records` | Records replayed from the restore point at the last start |
| `fix_source_journal_paused` | 1 while intake pauses on a full journal |
| `fix_source_feed_incomplete` | 1 after an unrecoverable reset in this run |
| `fix_source_messages_captured_total`, `fix_source_messages_excluded_total` | Journaled and unselected application messages |
| `fix_source_duplicates_total`, `fix_source_duplicates_unverified_total`, `fix_source_duplicate_conflicts_total` | Retransmission handling |
| `fix_source_rejects_sent_total`, `fix_source_gap_fill_seqs_total`, `fix_source_sequence_resets_total` | Session recovery evidence |
| `fix_source_rows_emitted_total`, `fix_source_projection_errors_total` | Projection |

Pipeline health follows the usual rules: a terminal source error faults the pipeline; reconnects
are internal to the engine and do not fault it.

## Performance

Measured 2026-10-14 on one Windows 11 laptop (consumer NVMe, `--release`), local QuickFIX acceptor
on loopback, ExecutionReports of about 200 bytes, one session. Latency is from the scheduled send
time (open loop) to the row returned by the source, so queueing delay is included. These are
local numbers, not a target: rerun on production hardware before sizing.

| Measurement | `journal.sync = os` | `journal.sync = fsync` |
|---|---|---|
| Journal append (300-byte record) p50 / p99 / p99.9 | 4.5 µs / 16.8 µs / 43 µs | 6.3 ms / 15.5 ms / 28.8 ms |
| Receive-to-Arrow at 500 msg/s p50 / p99 / p99.9 | 130 µs / 254 µs / 3.7 ms | saturated (see below) |
| Receive-to-Arrow at 2000 msg/s p50 / p99 / p99.9 | 115 µs / 596 µs / 16.6 ms | saturated |
| Sustained capture rate | above 14,000 msg/s (5,000-message burst in 340 ms) | 462 msg/s |

Projection costs 12 µs per message without groups and 21 µs / 43 µs with 3 / 10 expanded entries
(about 85,000 / 140,000 / 233,000 rows per second on one core).

`fsync` throughput is one `FlushFileBuffers` per message on this machine, because each message is
made durable before the engine may advance its sequence number. Server SSDs and `fdatasync` on
Linux are typically one to two orders of magnitude faster; measure before relying on `fsync` for
bursts. Receive-to-query and receive-to-committed-sink latency add the pipeline cycle and the
checkpoint interval respectively and were not measured separately.


- **Reconnect.** The engine reconnects every `reconnect.interval.seconds` (fixed interval) inside
  the schedule. Nothing else retries.
- **Certificate rotation.** Replace the CA or client files and restart the pipeline; TLS settings
  are read at start.
- **Disk pressure.** Watch `fix_source_journal_retained_bytes` against `journal.max.bytes` and the
  filesystem. A full journal pauses intake; it never drops records. Retention needs successful
  checkpoints. While paused, the source logs out every inbound message and logon attempt, so the
  engine logs repeated Logout/reconnect cycles at `reconnect.interval.seconds`; that is the pause,
  not a fault.
- **Stale data.** `fix_source_logged_on = 0` with growing `fix_source_messages_captured_total`
  stalled means the session is down; check engine events at debug level
  (`laminar_connectors::fix`).
- **Unresolved gaps / sequence reset.** Never reset sequence numbers to get connected. Agree a reset
  with the venue, then either set `reset.on.logon = true` for the next logon or stop the source and
  remove `journal.path/engine` (this starts a new series). Both are explicit operator actions.
- **Replay.** Restart from the last checkpoint replays the retained journal. To reprocess with a new
  mapping, drop and recreate the source with the same `journal.path`.
- **Failover.** Not automatic. To move a source to another host, stop the pipeline, copy the whole
  `journal.path` directory, and start it there; never run two copies.
- **Upgrade and rollback.** The journal format is versioned (`journal.json`); an unsupported format
  fails at start. Roll back by restoring the previous binary with the same directory.
- **Access control.** `journal.path` holds the counterparty's application messages; restrict it to
  the LaminarDB service account and include it in backups only where your data policy allows.

## Licence notice

The engine is QuickFIX C++, under the QuickFIX Software License 1.0. Distributions built with the
`fix` feature must include its notice and: "This product includes software developed by
quickfixengine.org (http://www.quickfixengine.org/)." See `vendor/quickfix-ffi/VENDORED.md`.
