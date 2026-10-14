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
| Deployment | Embedded, single-node server | Cluster (the journal and session state are node-local) |
| Delivery | At-least-once with checkpointing | Best-effort, exactly-once |
| Outbound traffic | Session messages only (Logon, Heartbeat, ResendRequest, Logout, Reject) | Application messages, subscription requests |
| Host failure | `journal.sync = fsync` survives process, OS, and power failure of the host | Loss of the node or its disk |

Validation evidence is local: a real QuickFIX acceptor on loopback with a synthetic dictionary
(`crates/laminar-connectors/tests/fixtures/fix/FIX44-synthetic.xml`). It is not venue
certification. Qualify every venue with its own dictionary, schedule, and conformance tests.

## How it works

1. The engine validates each inbound message against the dictionary and checks CompIDs,
   SendingTime, and sequence number.
2. For a selected message type, the source appends the engine's serialization of the message to
   the journal inside the engine callback. If the append fails, the session logs out without
   advancing its inbound sequence number, and the counterparty retransmits after reconnecting.
3. The session's sequence numbers live in a store the source owns (`session.identity` and
   `session.inbound` under `journal.path`). The persisted inbound sequence number never passes a
   journal record that is not yet durable. With `journal.sync = fsync` a background thread makes
   everything appended so far durable with one flush (group commit); records that start or reset
   a sequence series are flushed before the callback returns. The outbound sequence number is
   flushed before each message is sent, and a store reset first flushes the journal.
4. The source reads durable journal records, parses them with the pinned dictionary, and emits
   rows. Each row is positioned by `(session, journal index, row ordinal)`. A checkpoint stores
   the journal index; committed checkpoints release whole journal segments.
5. On restart the source replays the journal from the committed index without contacting the
   counterparty, and the session resumes from its persisted sequence numbers; the counterparty
   retransmits anything after them. Retransmissions of already journaled sequence numbers are
   dropped after a content comparison that ignores retransmission headers; a retransmission with
   different content stops the source.

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
| `journal.path` | required | Journal, session state, and pinned dictionary directory; exclusively locked |
| `journal.sync` | `fsync` | `fsync` (host failure; group-committed flushes) or `os` (process failure only) |
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
| Process killed at any point | No accepted message lost; the session resumes from its persisted sequence numbers and the counterparty retransmits anything after them; retransmissions are deduplicated |
| Host or power failure, `journal.sync = fsync` | As above. Records not yet flushed are lost from the journal but the persisted inbound sequence number never passed them, so they are retransmitted; an outbound sequence number is never reused |
| Host or power failure, `journal.sync = os` | Not covered: journal records may be lost; a restore past the journal end fails |
| Node or disk loss | Not covered: the journal and session state are on that node |
| Journal write or flush failure | The writer stops (no retry after an ambiguous flush); the session logs out without advancing; the source fails; restart truncates a torn tail |
| Session state write failure | The session stops: inbound processing disconnects, outbound messages are not sent, and the source fails |
| Damaged session state | `session.identity` with no valid copy fails the start (the outbound sequence number is unknown); a damaged `session.inbound` requests retransmission from 1 |
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
- A series starts when an accepted Logon carries ResetSeqNumFlag=Y or the session store was reset
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

Measured 2026-10-14 in `--release` on one laptop: Windows 11 on its consumer NVMe, and Ubuntu
under WSL2 on the same machine (ext4 on a virtual disk). Local QuickFIX acceptor on loopback,
ExecutionReports of about 200 bytes, one session. Latency is from the scheduled send time (open
loop) to the row returned by the source, so queueing delay is included. These are local numbers,
not a target: rerun on production hardware before sizing.

| Measurement | Windows `os` | Windows `fsync` | Linux `os` | Linux `fsync` |
|---|---|---|---|---|
| Engine-thread journal append, p50 / p99 | 4.5 µs / 18.9 µs | 0.1 µs / 1.1 µs | 0.7 µs / 3.0 µs | 0.07 µs / 1.4 µs |
| Receive-to-Arrow at 500 msg/s, p50 / p99 | 162 µs / 260 µs | 3.4 ms / 5.2 ms | 116 µs / 190 µs | 24 ms / 72 ms |
| Receive-to-Arrow at 2000 msg/s, p50 / p99 | 156 µs / 249 µs | 3.4 ms / 8.3 ms | 115 µs / 202 µs | 25 ms / 312 ms |
| 5,000-message burst, all rows | 14,600 msg/s | 14,100 msg/s | 53,000 msg/s | 27,200 msg/s |

With `fsync`, an append only buffers the record; rows become readable after the next group
flush, so receive-to-Arrow latency is about one flush on the storage (a few milliseconds on the
NVMe, about 20 ms on the WSL2 virtual disk) and throughput no longer depends on flush latency.
Receive-to-query and receive-to-committed-sink latency add the pipeline cycle and the checkpoint
interval respectively and were not measured separately.

Projection costs 12 µs per message without groups and 21 µs / 43 µs with 3 / 10 expanded entries
(about 85,000 / 140,000 / 233,000 rows per second on one core).

## Operations

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
  remove `journal.path/session.identity` and `journal.path/session.inbound` (this starts a new
  series at sequence number 1). Both are explicit operator actions.
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
