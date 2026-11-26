# Changelog

## [Unreleased]

### Added

- In cluster mode the PostgreSQL upsert sink (`write.mode = 'upsert'`, with or without
  `changelog.mode`) and the MongoDB upsert sink run at-least-once, each node writing the keys it
  owns, when each key is produced on one node at a time: the input is partitioned by columns that
  are all in the sink's key (a keyed aggregate into a sink keyed by its group), sits on one vnode
  (a global aggregate), or is projected from one source that runs on one node at a time (a
  PostgreSQL CDC `changelog` source replicated into a table keyed by its primary key). Other inputs
  are rejected before the pipeline starts, when the cluster plans its topology. In a cluster a
  PostgreSQL `changelog.mode` target may have no unique or exclusion constraint other than the
  primary key, and the sink's sessions end a transaction left idle for half of
  `statement.timeout.ms`. A MongoDB write sent just before a process's lease expired can still
  land after its successor's and leave the older value until the key's next change. In a cluster a
  MongoDB upsert collection needs a unique index on exactly its key fields and the simple
  collation, and its key may not be a microsecond or nanosecond timestamp, which MongoDB stores in
  milliseconds. Delta
  upsert stays rejected in a cluster. A source started on another node after a committed handoff
  no longer repeats its output, so a rescale during a PostgreSQL `changelog` copy resumes the
  copy. Connector API: `SinkTopology::KeyPartitioned`, `SinkConnector::destination_key` and
  `SinkConnector::share_destination_by_key`.
- In cluster mode the Kafka upsert sink (`envelope = 'upsert'`) runs at-least-once on input placed
  by its key, like the PostgreSQL and MongoDB upsert sinks. Each process writes through its own
  transactional producer, one transaction per written batch, under the `transactional.id`
  `laminardb-writer-<sink>-<node>-<boot id>`. Before a recovery hands a failed process's keys to a
  successor, the leader initializes transactions under that id, so nothing the failed process
  still sends can land after its successor's record. The brokers must support transactions, and
  the sink's principal needs `Write` and `Describe` on transactional ids with the prefix
  `laminardb-writer-`. A frozen process's open transaction holds back `read_committed` consumers
  until the fence or `transaction.timeout.ms`, and a recovery that removes a process, a cold start
  of the cluster included, waits for the brokers when they are unreachable. Embedded and single-node Kafka sinks are unchanged. Connector
  API: `SinkConnector::share_destination_by_key` takes the process's `DestinationWriter`, and a
  sink connector type registers a fence for departed writers with
  `ConnectorRegistry::register_sink_writer_fence`.

- In cluster mode a PostgreSQL CDC source runs at-least-once on one node at a time in both output
  modes, with or without the initial copy. A `changelog` source feeds aggregates, materialized
  views and Delta `append` sinks; an `upsert` source with the copy feeds an interval join, as one
  without the copy already did. When the node copying the table fails or gives the source up, the
  next owner continues the copy from the committed checkpoint. A restart inside a `changelog`
  copy now resumes it in every mode unless an at-least-once sink that applies changes by key
  reads the source, directly or through streams (PostgreSQL, Kafka `envelope = 'upsert'`,
  MongoDB or Delta upsert sinks, or a sink without a connector), or a stream on the way whose
  SQL cannot be parsed, since its inputs are unknown: the resumed copy reads its next chunks at a
  newer snapshot, so such a sink could keep a row deleted while the copy was down, and there the
  restart still fails closed with the reset. Adding a source that copies to a running cluster and exactly-once
  stay rejected. `SourceStart` carries a `ReplayRequirement` (`with_replay_requirement`,
  `replay_requirement`) that the engine derives from the sinks reading each source, and
  `SinkContract::with_appended_changes` marks a sink that logs each change, such as Delta
  `append`, instead of applying it by key.

- A Delta Lake change feed (`read.mode = 'changes'`) is certified for exactly-once delivery. Its
  cursor stands only between whole commits and snapshot files, a replayed commit or file
  reproduces its rows in the same order, and missing or regressed history fails the read. Its
  changes carry retractions, so an exactly-once pipeline writes them to a sink that takes a full
  changelog, such as Delta Lake `append`, which keeps each change's `__weight`; in cluster mode
  that is the one such sink, on direct S3/S3A, and a feed on object storage or a Unity Catalog
  `uc://` table still runs on one node at a time. The Iceberg append sink takes no changelog.
  Under exactly-once a checkpoint that cannot reach the end of the commit or file being read
  within the checkpoint timeout faults the pipeline, so keep the timeout above the time to read
  the largest commit or snapshot file. `read.mode = 'snapshot'` stays at-least-once.

- In cluster mode, a Delta Lake change feed on object storage (or a Unity Catalog `uc://` table)
  runs at-least-once on one node at a time and feeds aggregates, materialized views and Delta
  `append` sinks, as it does in embedded mode. Full-changelog sources now have a cluster route:
  one node reads a changelog in log order, and the aggregates and views over it spread across
  the nodes by their keys. A survivor resumes the feed from the committed cursor, and a rescale
  moves it at the handoff checkpoint. A local `table.path` is now rejected in cluster mode as
  node-local storage; `read.mode = 'snapshot'` and adding the source to a running cluster stay
  rejected there.

- A PostgreSQL CDC source takes `change.time.column`: a declared, non-key `TIMESTAMP NOT NULL`
  column that is not a table column, filled with each change's transaction commit time to the
  millisecond, deletes included, with `snapshot.mode=never`. With a watermark on it, an `upsert`
  source can be the right side of a temporal join in every mode; before, its deletes carried only
  the key, so no non-null time column could hold, and the table's own timestamps kept
  microseconds a temporal join refuses. In cluster mode an `upsert` source without the initial
  copy runs at-least-once on one node at a time, may start sealed, and feeds a temporal join's
  right side, or an interval join when its key holds every join column and the event time. A node
  ends another session of its slot only once that node is fenced, and checks its own process lease
  just before it opens a session, creates the slot or ends a session, so a node that resumes after
  its lease expired does not take the slot back (the check is not atomic with the call, so a stall
  between the two can still let that one call through). `SourceStart` carries the cluster
  `ProcessLeaseCheck` (`with_process_lease`, `process_lease`, `with_position`). Exactly-once
  stays rejected in cluster mode.

- The generator source takes `splits = n`, which spreads its sequence over `n` input channels:
  row `i` is on channel `i mod n`. In cluster mode each channel is read by the node that owns the
  vnode a versioned, engine-owned route gives it, so a split generator runs on several nodes. A
  split generator runs at-least-once in every mode and is not certified for exactly-once; without
  `splits` the generator is unchanged. `SourceContract` gains `placement`
  (`with_placement(SourceChannelPlacement { channels, route })`), which enters pipeline identity
  only when a source declares it.

- In cluster mode, a RabbitMQ source (classic or quorum queue) and a MongoDB change-stream source
  with `output.mode = history` and `snapshot.mode = never` run at-least-once on one node at a
  time: the node that owns the source's home vnode. When that node fails, a survivor takes the
  source over from the committed checkpoint; when the source moves at a rescale or a rejoin, the
  node giving it up stops at the handoff checkpoint and the next one resumes there. A topology
  migration can add either source to a running cluster. RabbitMQ with `FORMAT FPML`, MongoDB
  `document` mode and MongoDB's initial snapshot stay rejected in cluster mode.

- In cluster mode an IBM MQ source runs at-least-once on one node at a time, as a RabbitMQ
  source does. At a planned move the node giving the source up commits the handoff
  checkpoint's unit of work before it closes; when the owner fails, the queue manager backs out
  its unit of work and the next owner gets those messages again. An IBM MQ source may start
  sealed in every mode (it reads what the queue holds). A cluster source must set
  `queue.manager`, and every node must reach that one queue manager (a name alone does not
  guarantee it); without the option, and with `FORMAT FPML`, the source stays rejected in cluster
  mode. A placed singleton source that binds its own cursor to each batch is no longer asked
  for a checkpoint per batch, so an IBM MQ source reads on between checkpoints in cluster
  mode.

- A MongoDB document source takes `change.time.column`: a declared, non-key
  `TIMESTAMP NOT NULL` column filled with each change's server wall time (MongoDB 6.0 or later),
  deletes included. With a watermark on it, the source can be the right side of a temporal join
  in every mode; before, a document source could not have a non-null event-time column, and a
  watermarked one faulted at its first batch because its checkpoints named no input channel. In
  cluster mode a document source without the initial copy runs at-least-once on one node at a
  time, as history mode does, and may feed a temporal join's right side, or an interval join
  when its key holds every join column and the event time (a `TIMESTAMP` `_id`). A placed
  singleton's positioned rows now carry its one channel, so any such source can feed a temporal
  or interval join in a cluster.

- The Iceberg source takes `read.splits = n` in append mode. Each data file belongs to one of `n`
  input channels by a hash of its path, and each channel keeps its own position in the snapshot
  lineage, so a cluster reads one table on several nodes, each channel on the node that owns the
  vnode the engine's route gives it. A split Iceberg read runs at-least-once in every mode; its
  checkpoint has a new format, so a source cannot resume across adding or removing `read.splits`.
  Without `read.splits` the source is unchanged.

- The files source reads an object-store directory: `path` may be an `s3://`, `s3a://`, `gs://`,
  `az://`, `abfss://` or `wasbs://` URL, with credentials from `storage.*` options or the
  environment, as for Delta Lake. The source lists the directory's direct children every
  `poll_interval` (which must be greater than zero), reads new objects in key order, and records
  each object's key relative to the directory in its checkpoint, so a resume reads only new objects
  and continues a partly read one at its next row. Object-store paths take CSV, JSON or text with
  declared columns; Parquet, Arrow IPC, FpML and schema inference still need a local path, and the
  files sink stays local. Such a source is at-least-once in every mode.

  With `splits = n` (object-store directories only) each object belongs to one of `n` input
  channels by a hash of its key, and each channel keeps its own processed keys and partly read
  object. In cluster mode each channel is read by the node that owns the vnode the engine's route
  gives it, so several nodes read one directory. Without `splits` cluster mode rejects the source.
  A split source's checkpoint has a new format, so a source cannot resume across adding or
  removing `splits`.

### Changed

- In cluster mode, while a node's subscription output is held at its commit high water (three
  quarters of the 256 MiB bound), an aggregate without `EMIT CHANGES` applies its input without
  emitting a full snapshot each cycle, and the next checkpoint emits one snapshot of the result.
  Readers see only committed cuts, so the snapshots this skips were superseded before anyone could
  read them. Previously each cycle staged a snapshot of every owned group, and a long checkpoint
  hold, such as a topology request being admitted, could pass the bound and fault the pipeline.
  Sinks and streams fed by such an aggregate get its updates at checkpoint cadence while output is
  held; ordinary operation is unchanged, and so are `EMIT CHANGES` aggregates. The high-water
  warning now names the five streams holding the most pending output. The hold covers every such
  aggregate on the node, as the intake pause does (#712, #677).
- `SourceConnector::finish_drain` takes a `SourceDrainSettlement` instead of a
  `SourceDrainResolution`. A commit carries the source's merged cursor at the committed handoff
  checkpoint, and a connector that implements cluster drains positions every input it acquires
  from that cursor. `SourceDrainSettlement::resolution()` returns the round and outcome.
- In cluster mode, a FIX source or a Files source is rejected with a reason naming the state that
  ties it to one node: the FIX receive journal or the local input files. Both were already
  rejected, with a generic singleton-placement message. `SourceContract` gains a `locality`
  field, set with `with_locality(SourceLocality::…)`; it defaults to `Portable`.
- Cluster exactly-once now requires a source certified for it across cluster nodes, in addition
  to the single-node certification. Kafka is the only such source, so the admitted compositions
  are unchanged; a connector that later splits across nodes runs exactly-once in cluster only
  after its own cluster certification. `SourceContract` gains
  `is_cluster_exact_delivery_certified()`.

### Fixed

- In cluster mode, a read or write of the shared checkpoint store no longer fails with "error
  sending request" right after a node restarts its pipeline, as it does in a recovery round or a
  topology migration. Each pipeline's compute runtime reads the store, and connections it opened
  stopped working when it ended, so a request sent on one failed without a retry. The cluster's
  store now runs all of its HTTP requests on the server runtime
  (`object_store_builder::build_object_store_on`).

- In cluster mode, a graceful vnode rotation accepts as its handoff only a checkpoint that carries
  the handoff, which fences each giving node's intake before its state is captured. Before, if the
  drain ended just as the rotation's checkpoint was admitted, an ordinary checkpoint could commit
  in its place and the rotation could proceed while nodes giving up vnodes still accepted input
  for them. The rotation now fails and aborts the drain instead.

- In cluster mode, a node that owns vnodes and rejoins while a coordinated recovery holds it no
  longer fails startup with "cluster assignment N is not certified for source intake". The
  recovery withdraws the node's assignment fence; startup now keeps intake closed and waits for the
  round to release it, as it already did when the fence was withdrawn later in startup.

- Sinks that collapse a weighted changelog by key (PostgreSQL `changelog.mode`, MongoDB and Delta
  upsert, Kafka `envelope = 'upsert'`) no longer drop a key whose changes cancel within one flush,
  such as a row inserted and then deleted. They now apply its last change: a delete after a final
  retraction, the row after a final insertion. Before, an at-least-once replay that flushed both
  could leave behind the row a failed attempt had already written. A Kafka upsert sink can now
  write a tombstone for a key it never published.

- A PostgreSQL CDC checkpoint no longer falls inside a chunk of the initial copy that is partly
  emitted. Before, such a cut held rows the cursor did not cover, and the resumed copy read the
  chunk again; harmless for upserts, it would have counted those rows twice in a changelog. A
  checkpoint now waits for the rest of the chunk, which is already in memory. All modes.

- In cluster mode, a placed source that offers a cursor only between replay units (a Delta Lake
  change feed, a PostgreSQL copy) now reads the unit it is in to its end when a rescale or rejoin
  drains it. Before, the drain stopped reading at once and the handoff checkpoint could not be
  captured inside a commit or snapshot file, so the drain ran out its preparation deadline.

- A Delta Lake source whose table log ends before a version its cursor already reached (a table
  directory restored from an older copy keeps its table id) now fails with a terminal error
  that asks for a reset. Before, a source resumed from such a cursor waited for the log to catch
  up and then skipped the new commits at the versions it had passed; a running source retried its
  listing indefinitely, or skipped them the same way after renewing vended credentials. All modes.

- In cluster mode, a checkpoint drain no longer stages one full snapshot of a certified aggregate
  subscription (one without `EMIT CHANGES`) for every queued peer shuffle event. The aggregate
  applies the events without emitting, and a final settle pass, run once the drain's other work is
  done, emits one snapshot of the result before the cut. Events still queued then stay as replay
  state, as before. Previously a topology-migration cut could fill the bounded subscription output
  ("cluster subscription total pending output reached its bounded … limit") and abort the
  migration. Cluster mode only (#712).

- The NATS sink checks `subject.column`, `header.columns` and `dedup.id.column` at `CREATE SINK`:
  each must name a `Utf8` or `Utf8View` query column. Before, a missing or wrongly typed column
  passed `CREATE SINK` and faulted the pipeline at its first batch. With `FORMAT RAW`, the error notes
  that these columns count toward its single payload column. This applies in every deployment mode
  (#745).

- The PostgreSQL sink writes `Utf8View` columns as `TEXT` and `BinaryView` columns as `BYTEA`, in
  append (COPY) and upsert (UNNEST) mode. A SQL string cast such as `CAST(x AS VARCHAR)` produces
  `Utf8View`, so a pipeline that wrote a cast string column into a PostgreSQL sink failed at
  startup with "PostgreSQL sink does not support Arrow type Utf8View" (#740).

- The Delta Lake and Iceberg sinks write `Utf8View` and `BinaryView` columns, which a SQL string
  cast such as `CAST(x AS VARCHAR)` produces, into the table's string and binary types. Before,
  `CREATE SINK` failed: Delta refused to create the table ("Delta native schema changes query types
  or nullability") or to bind an existing one, and Iceberg refused to bind the table ("field
  differs in type or directional nullability"). Delta writes them in append, upsert and
  exactly-once append mode, Iceberg in every write mode, in every deployment mode (#741).

- The `raw` format and the RabbitMQ, NATS and IBM MQ property columns (routing key, subject,
  message, correlation and dedup ids, headers, properties) take `Utf8View` columns, and
  `BinaryView` wherever they take `Binary`; the MongoDB sink writes `Utf8View` as a BSON string,
  upsert keys included. A raw text payload from a view keeps its text content type (`text/plain`
  on RabbitMQ, `MQSTR` on IBM MQ). Before, `CREATE SINK` failed ("unsupported Arrow type
  Utf8View", "property column must be Utf8"), or for NATS the first batch faulted. This applies in
  every deployment mode (#741).

- A changelog `_op` column that a stream builds with a SQL string cast such as `CAST(x AS VARCHAR)`,
  which produces `Utf8View`, drives upserts and deletes in the Delta Lake, PostgreSQL and Kafka
  upsert sinks and passes the append-only interval join's check; the AI inference operator reads a
  `Utf8View` input column. Before, PostgreSQL refused `_op` at `CREATE SINK`, the others faulted at
  the first batch ("_op column is not Utf8", "input column must be Utf8"). This applies in every
  deployment mode (#746).

- A pipeline over a PostgreSQL CDC or Delta Lake change data feed source with a `WATERMARK`
  faulted at the source's first batch ("ordered event-time source checkpoint omitted its
  input-channel inventory"). Their checkpoints now name the one input channel their rows use: the
  replication slot for PostgreSQL, and the table's `metaData.id` for Delta, whose rows were
  positioned under an empty channel. Sources without a watermark behave as before and resume from
  existing checkpoints. Embedded and single-node; both sources stay rejected in cluster mode.

- A stream that selects source columns carrying field metadata, such as an Iceberg column's field
  ID, emitted batches without that metadata while its planned schema kept it, so a MongoDB or files
  sink reading the stream rejected every batch and the pipeline kept recovering. The compiled
  projection now emits the planned fields.

- A MongoDB CDC source could not resume from a checkpoint when its resume token carried binary
  `_typeBits`, as it does when the collection's keys are 64-bit integers: the token was stored as
  plain JSON, which turned `_typeBits` into an array of numbers, and MongoDB rejected it (error
  40648) on every restart. Tokens are now stored as canonical Extended JSON, in checkpoints and in
  the `resume_token` column of history records. Tokens without `_typeBits` are stored as before;
  a checkpoint already written with the broken form still cannot be resumed.
- In cluster mode, a Kafka partition that moved live between two running nodes, for example when
  a node drained for a graceful shutdown, was read again from where the new owner last knew it,
  or from the start of the partition, instead of after the records the previous owner had
  processed. At-least-once pipelines counted those records twice in their state, and
  exactly-once pipelines published them twice. The new owner now resumes the partition from the
  committed handoff checkpoint. Recovery after a node is killed was not affected.
- quinn-proto moves to 0.11.19 for RUSTSEC-2026-0523 (a MAX_STREAM_DATA frame could open streams
  beyond the stream limit) and RUSTSEC-2026-0524 (a long Retry token could panic or exhaust memory
  in a QUIC client), and quinn-udp to 0.5.16 for RUSTSEC-2026-0538 (the receive loop could spin
  after a truncated datagram).
- url moves to 2.5.9 for RUSTSEC-2026-0592 (`Url::origin()` overflowed the stack on nested
  `blob:` URLs).
- Arrow and Parquet move to 59.3.3, which fixes RUSTSEC-2026-0595: Parquet bloom filters were
  written as plaintext when writing encrypted Parquet.
- zstd moves to 0.13.5 and zstd-safe to 7.4.1, which fix RUSTSEC-2026-0391 and RUSTSEC-2026-0395
  to 0406 within the zstd versions Arrow, DataFusion and Parquet require. Their dependency
  exceptions are removed. The remaining exceptions are renewed to 2026-12-30.

## [0.40.2]

0.40.0 and 0.40.1 were tagged but never published, so this is the first release since 0.33.0
and carries everything below.

### Added

- Event time flows through interval joins and closed windows, in every mode. An interval join's
  output carries the time column of a side every output row has, when it projects that column
  unchanged: the left one for `INNER` (else the right), the left one for `LEFT`, `LEFT SEMI` and
  `LEFT ANTI`, the right one for the `RIGHT` kinds, and none for `FULL`. A closed `TUMBLE` or
  `HOP` window carries its projected end marker, or else its start marker. `EXPLAIN` shows the
  column.
- An interval join can read another interval join's stream on that stream's event-time column,
  in every mode.
- Cluster mode admits a window over an interval join stream and a window over a closed window
  stream, on the event-time column the input carries.
- An append-only view over an interval join keeps an interval on the join's event-time column, so
  it no longer has to declare `retention = '<n> rows'`.
- A `UNION ALL` carries its inputs' event-time column, in every mode, when every branch forwards it
  unchanged to the same column from the same kind of operator; its watermark is the lowest of its
  inputs' watermarks, and `EXPLAIN` shows the column. An interval join may read such a union, and
  an append-only view over it keeps an interval on the column instead of declaring a row count.
- Cluster mode admits a `UNION ALL` whose every branch forwards one source event-time column
  through projections and filters, and windows, interval joins, aggregates and views over it.
  Each node unions the rows it holds, and a window or join over the union closes on the lowest of
  its sources' node-local watermarks. A union over a join or window stream, or one without the
  event-time column, is still rejected.
- A chain of interval joins over three or more named inputs (`a JOIN b ON .. JOIN c ON ..`, each
  `ON` with a `BETWEEN` bound) runs in every mode, in a stream or a materialized view. It is split
  into one internal stage per join, each admitted as a two-way join over the stage before it, so
  each later join must bound its time by the event-time column the earlier joins carry; `CREATE`
  names the column otherwise. In cluster mode the chain counts toward the three-exchange depth
  limit like named stages.

- `MIN` and `MAX` work over changelog inputs: an `EMIT CHANGES` aggregate, a changelog
  materialized view, a CDC or change-feed source, and the second stage of a salted aggregate.
  Each group keeps every value it holds with its net weight, so retracting the current extreme
  falls back to the next one; the values count toward `managed_state_bytes` and the memory
  budgets, in every mode, with or without `GROUP BY`. In cluster mode a global (ungrouped) one
  has every node send its rows to the owner of vnode 0 (`EXPLAIN` shows `gather to vnode 0`).

### Changed

- A multi-way join that is not a chain of interval joins over named inputs (with a lookup or
  temporal table, `USING`, a subquery input, or no interval bound) is still rejected, with a
  message that says which multi-way joins split. Cluster mode reports a window over a join that
  cannot be split with the one-input window message instead of the windowed-join message.
- `CREATE` rejects a window over a window stream that groups by any column other than the one the
  upstream carries (for example its start when it projects its end), and a window over a session
  window or over a window that projects no boundary. Such a window could close before the
  upstream emitted into it. The check runs whenever the DDL runs, so after upgrading, an embedded
  or single-node pipeline with such a window fails to start until it windows on the carried
  column. Cluster mode never admitted these windows.
- `TUMBLE`, `HOP` and `SESSION` windows keep their `COUNT`, `SUM`, `AVG`, `MIN` and `MAX` state
  in the same form as non-windowed aggregates, in every mode. A windowed `SUM` or `AVG` whose
  state would overflow now fails the operator's cycle with an error, as a non-windowed one does,
  instead of wrapping. A floating-point `SUM` or `AVG` adds its values in row order, so a result
  can differ in its last bits from earlier releases.
- Cluster mode: an aggregate, window, interval join or temporal join applies a cycle's local rows,
  and a window closes, in the cycle that sends its rows and watermark to the other nodes when the
  shuffle accepts them without waiting. Before, every cycle that advanced the watermark applied
  its local rows on the following cycle and held new input until then, even when nothing went to
  another node. A send that has to wait (a new connection, a full send budget) still finishes in
  the background, and the rows apply on a later cycle as before.
- Arrow and Parquet 59.3.2, DataFusion 55.2.0, Delta Lake 1.1 (Delta kernel 0.28.1) and Iceberg
  0.11.0. Iceberg is published as the `laminardb-iceberg*` crates, the upstream 0.11.0 sources on
  Arrow 59, until an upstream release shares Delta Lake's Arrow version.
- A string compared with a number now converts the string to a number, in every mode:
  `int_col = 'hello'` fails with a cast error instead of matching no rows, and
  `str_col IN ('a', 1)` is rejected. In a `UNION ALL`, a computed column needs an alias when
  another column in the same branch would share its output name.
- The Iceberg sink writes a partition value containing `/`, `\` or `?` URL-encoded inside one
  path segment instead of refusing it.
- The Delta sink refuses a table that uses column mapping when it opens.

### Fixed

- rustix moves to 1.1.6 for RUSTSEC-2026-0486 (xattr functions marked unwritten bytes as
  initialized when given a zero-length buffer).
- An exactly-once Delta Lake sink now writes a Delta checkpoint every 10 commits, or at the
  table's `delta.checkpointInterval` when it sets one, in every mode. It used to write none, so
  each checkpoint's Delta commit read the table's whole log several times and took longer the
  longer the pipeline ran. It also reads the log once less per commit.
- The macOS server binary no longer aborts at startup with "option background_thread currently
  supports pthread only"; jemalloc background threads stay enabled on Linux.
- In cluster mode a `UNION ALL` that reads one source twice passed `CREATE` and was then refused
  when the pipeline started (`[LDB-6066]`, "SQL execution shape has not been classified"); it now
  runs.
- In every mode, `CREATE` certifies an aggregate over a changelog materialized view in its
  changelog form, as it already did over an `EMIT CHANGES` stream. One whose changelog state
  cannot hold its input (for example `MIN` or `MAX` over an `INTERVAL` column) is now rejected at
  `CREATE`; before, it was admitted and the pipeline failed to start.
- A window over another window's output no longer drops the upstream's rows. A window stream's
  watermark now trails the watermark its windows close at: by the allowed lateness when it
  projects `tumble_end` or `hop_end`, and by the lateness plus the window size otherwise.
  Downstream windows, append-view retention and `stream_watermark_ms` see the held watermark.
  Session windows are not held. This applies in every mode.
- An append-only view that projects a window's end but not its start (`tumble_end` or `hop_end`
  grouped with `tumble` or `hop`) keeps an interval on the end column. `CREATE` used to fail for
  lack of an event-time column. A `TUMBLE` or `HOP` view now keeps a window while the window's end
  is within the interval of the watermark its windows close at, less the allowed lateness,
  whichever boundary it projects.
- An append-only session view keeps a session while its start is within the interval plus the gap
  and the allowed lateness. A session view whose `ALLOW LATENESS` exceeded its retention used to
  drop every session as it closed.
- `ALLOW LATENESS` is applied. It used to be parsed after the `EMIT` clause and then ignored. It
  now keeps windows open in embedded and single-node mode. It must follow `EMIT ON WINDOW CLOSE`,
  and `CREATE` rejects it with `EMIT FINAL`, with other `EMIT` modes, on a query without a
  window, and in cluster mode. Any other text after the `EMIT` clause is now a parse error.

### Upgrade

- A stream or view whose DDL already carried `ALLOW LATENESS` gets a new pipeline identity, so its
  existing checkpoints fail closed; it restarts from its sources.
- The state frame ABI is now 9, for the window state change above. Every checkpoint written by an
  earlier release fails closed at startup, in every pipeline and mode, and the pipeline restarts
  from its sources.
- Delta tables on S3 no longer accept DynamoDB locking: `storage.s3_locking_provider`,
  `storage.dynamodb_table_name`, `AWS_S3_LOCKING_PROVIDER` and `DELTA_DYNAMO_TABLE_NAME` are
  rejected for reads and writes. delta-rs 1.0 removed its DynamoDB log store; S3 commits use
  conditional put. Remove these settings before upgrading.
- Cluster nodes admit only peers of the same version, so a cluster moves to 0.40.2 by restarting
  every node. Checkpoints written by 0.34.0 restore.
- Applications embedding `laminar-db` move to Arrow 59 and DataFusion 55 with it.

## [0.33.0]

### Added

- RabbitMQ source and sink (`rabbitmq` feature, off by default) over AMQP 0-9-1. The source
  acknowledges deliveries only after the checkpoint holding them is durable and can dead-letter
  undecodable messages; the sink publishes with confirms and `mandatory` and drains every pending
  confirm before a checkpoint commits. See `docs/RABBITMQ.md`.
- IBM MQ source and sink (`ibmmq` feature, off by default; IBM's client library is loaded at run
  time and never bundled). The source commits its unit of work only when a durable checkpoint
  covers it and can move poison messages to the backout queue in the same unit of work; the sink
  commits bounded units of work per write. See `docs/IBMMQ.md`.
- Both message sources run embedded and single-node; cluster mode rejects them. Both sinks also
  run in cluster mode as at-least-once writers.
- `FORMAT RAW` with a single Binary `value` column keeps payload bytes exactly.
- FIX TagValue source (`fix` feature, off by default) on the QuickFIX C++ engine: one initiator
  session per source (FIX.4.2, FIX.4.4, or FIXT.1.1 with one FIX 5.0 application version) over
  verified TLS or loopback TCP, a group-committed receive journal written inside the engine
  callback, a source-owned session store that never persists the inbound sequence number past a
  record that is not yet durable, dictionary-validated column mappings for header, body, group,
  and metadata fields, deterministic replay, and at-least-once or exactly-once delivery in
  embedded and single-node deployments. Cluster placement is rejected. See `docs/FIX_SOURCE.md`.
- FpML documents (`fpml` feature, off by default) on the files, Kafka, RabbitMQ, and IBM MQ
  sources (`FORMAT FPML`), and in a FIX data field such as SecurityXML (`fpml.field`, an appended
  `fpml` struct column). Each document is validated against the bundled FpML 5.13 confirmation
  schema with a vendored, network-free libxml2, its `id`/`href` references are checked, and a
  profile turns it into typed rows with exact decimals and dates. Fourteen bundled profiles cover
  every product, loan representation, and document type in the schema; your own schemas and
  profiles extend them without code. Embedded and single-node only. See `docs/FPML_SOURCE.md`.
- Certified commit-coupled sources are admitted for exactly-once delivery; uncertified ones are
  still rejected.
- FIX source `logon.field.<tag>` for static venue-specific Logon fields, and
  `counterparty.resends = 'false'` for counterparties that do not retransmit: gap fills and
  new sequence series then flag the next row `gap_before`.
- FIX source `venue = 'binance'` profile for Binance Spot drop copy: an Ed25519-signed Logon, a
  sequence reset at every logon, and `gap_before` on the first row of every later series,
  because Binance does not retransmit.
- `examples/fix-desk`: a trading desk over a FIX drop copy in SQL only (positions, per-minute
  flow, rejects, large fills, order bursts, drop-copy delay, feed health, and an exactly-once
  Delta Lake audit trail reconciled in DuckDB), with one connection file per counterparty and a
  local broker simulator (`laminar-connectors` example `fix_venue`).

### Changed

- The prebuilt server binaries for Linux (glibc), macOS, and Windows include the `fix` and `fpml`
  features, and their archives carry the third-party notices in `licenses/`. The Linux musl
  binary does not include them yet.

### Fixed

- Kafka sources with `metadata.columns` restart on persistent storage: declared columns are
  checked as the prefix of the committed schema binding.
- Managed aggregates coerce mixed operand types in group keys, aggregate arguments, and `WHERE`
  (`SUM(CASE WHEN ... THEN decimal ELSE 0 END)`, `double * 1000`, `decimal >= 1`) instead of
  failing at runtime with `LDB-8051`. A `CASE` whose branches cannot be narrowed exactly to its
  first branch's type is rejected at `CREATE` with a request to `CAST` them.

## [0.32.0]

### Added

- Kafka exactly-once output in embedded and single-node deployments: each checkpoint's records are
  published by one fenced transactional committer together with a cursor marker in a compacted
  `commit.topic`, so `read_committed` readers see every committed record once across crashes.
  Cluster Kafka output remains at-least-once.
- Kafka `latency.profile` (`low_latency`, `balanced`, `throughput`), `group.protocol`
  (`classic`, `consumer`), sink `key.format` and `partition.count`, and source
  `metadata.columns` (`key`, `topic`, `partition`, `offset`, `timestamp`, `timestamp_type`,
  `headers`).

### Changed

- librdkafka is 2.16.0 through a vendored `rdkafka-sys`; zstd compression needs the `kafka-zstd`
  feature.
- `kafka.*` passthrough properties that override a connector-owned or protected setting are
  rejected instead of ignored, and SASL mechanisms, TLS and codecs are checked against the linked
  librdkafka before a client is created. OAUTHBEARER is rejected.
- Kafka sinks route keyed records to `murmur2(key) % partitions` explicitly; upsert sinks require
  `partition.count`. Kafka sources preserve NULL versus empty keys and values, keep binary,
  duplicate and NULL headers in order, and advance their cursor over records that produce no row.
- Guaranteed Kafka append sources reject compacted input topics when the broker reports the
  topic's `cleanup.policy`; without DescribeConfigs permission the source starts with a warning.
  The Avro writer-schema cache evicts least recently used schemas instead of failing after 64,
  and a Schema Registry client certificate applies without a private CA.

### Fixed

- A sink reading an incremental materialized view directly binds the view's changelog, including
  its weight column, so a keyed Kafka upsert sink over a `GROUP BY` view no longer fails with a
  schema mismatch on its first write.

### Removed

- Kafka source `include.metadata` and `include.headers`; use `metadata.columns`.

## [0.28.0]

### Changed

- Stateful joins and non-windowed keyed aggregates now use one managed vnode ownership,
  checkpoint, restore, and rebalance lifecycle in single-node and cluster execution.
- The bounded append-only event-time join supports `INNER`, left/right/full outer, left/right semi,
  and left/right anti joins with ordered `BIGINT`/`VARCHAR` composite keys. A named join stream may
  feed a separate non-`DISTINCT` `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` aggregate stage. Fused
  `JOIN ... GROUP BY`, cluster event-time window aggregates, and unsupported join shapes fail
  admission.
- Join output is capped at 262,144 rows and 64 MiB per cycle. Exceeding either cap is a terminal
  controlled failure; the operator does not spill or continue a partial fanout.
- Cluster exactly-once admission is connector-gated. The admitted composition is exact-certified
  Kafka input with coordinated append-mode Delta Lake on direct S3/S3A. Azure and GCS Delta targets
  remain cluster at-least-once pending provider-native EO soaks; Kafka and Iceberg sinks remain
  at-least-once.
- Checkpoint and recovery records now bind the exact deployment, pipeline, assignment, process
  roster, vnode inventory, source cut, state seal, and external publication cursor. Production
  certification remains pending the four-mode real-connector fault, validity, and latency soak
  matrix.
- The public `[state]` backend selector is removed; `[checkpoint]` is the sole durability
  configuration. Aggregate and window hot maps now use `FxHashMap`. The final one-node-object
  checkpoint cutover and production soak certification remain pending.
- Embedded, standalone, and cluster runtimes now share one configurable key-group topology with a
  default of 256. Local keyed aggregates and interval joins route across all locally owned vnodes;
  the unified v7 checkpoint image and remaining stateful operators are still pending.
- The v0.28 checkpoint and state format is intentionally incompatible with older persisted formats,
  which are rejected and require a fresh deployment namespace.

### Removed

- Removed the experimental tiered-state feature and configuration.
- Removed the public `VnodeRehydrator`, `VnodeRehydration`, and `RehydratedVnode` APIs and
  `LaminarDB::rehydrated_vnode_state()`. Restore is now an internal authority-pinned lifecycle.
- Removed obsolete WebSocket source-server, replay, connector-owned checkpoint, and connector-owned
  event-time options. WebSocket sources are client-only and best-effort; event time is declared by
  SQL watermarks.
- Removed superseded distributed-state and recovery plans from the active documentation set.

## [0.22.0]

### Added

- **Postgres wire protocol, production hardening**: full TLS path on
  the SUBSCRIBE listener: `pgwire_tls_min_version` to pin TLS 1.3
  (`bcab5471`, #12), optional mTLS via `pgwire_tls_client_ca` using
  `WebPkiClientVerifier` (`88e2af46`, #10), in-place hot reload of the
  `TlsAcceptor` via a `notify` watcher with debounce (`2cad3d54`, #11),
  and `pg_authid`-style pre-hashed `md5{32-hex}` passwords in
  `pgwire_users` so plaintext never has to live in `laminardb.toml`
  (`a3376408`, #7).
- **Extended-query + binary format** on the pgwire listener (`95fa8a74`).
- **SQL-level cursors**: `DECLARE c CURSOR FOR SUBSCRIBE …` +
  `FETCH n FROM c` for libpq clients with `\set FETCH_COUNT n`
  (`ab49408e`).
- **Explicit window-edge columns**: `tumble_end`, `hop_end`,
  `cumulate_end` UDFs return the window-end timestamp directly
  (`0a5b862e`).

### Changed

- **`DbState` collapse** (#381): removed the dual representation that
  carried both the in-flight `LaminarDB` handle and a snapshot of its
  contents. Trims docs/restated state.

## [0.21.0]

### Added

- **SUBSCRIBE over the Postgres wire protocol** (#369): `psql`, JDBC,
  asyncpg, and any libpq client can tail a stream or materialized view.
  Includes the listener itself, MD5 password auth, TLS, and a
  stream-side schema layer for `WHERE` push-down.
- **Pgwire hardening** (#371): connection caps, per-client throttling,
  certificate expiry warnings.
- **`RETAIN HISTORY` + `AS OF EPOCH`** (#373): `CREATE STREAM … WITH
  ('retain_history' = '64mb')` keeps a bounded ring of recent committed
  epochs in memory; `SUBSCRIBE … AS OF EPOCH n` resumes from epoch `n`
  so a reconnecting client doesn't miss rows produced during the
  disconnect.
- **Materialized view result storage and queryability** (#319): MV
  output is persisted via `mv_store` and exposed through a DataFusion
  `TableProvider`, so `SELECT * FROM mv_name` works after the pipeline
  has produced results.
- **Push-based WebSocket stream subscriptions** (#329): replaces the
  earlier poll loop on `/ws/{name}`.
- **Self-join pre-filtering** (#321): predicate analysis on
  `FROM a JOIN a …` cuts N² buffer growth before the join executes.
- **OTel demo + connector cleanup** (#317): schema auto-discovery,
  cleaner connector registration surface.
- **Production-grade Prometheus metrics** (#339): pipeline counters,
  cycle duration percentiles, checkpoint telemetry, sink error/timeout
  counters, per-connector gauges.
- **Throttled checkpoint barrier retries** (#327): back off rather
  than spin when a sink times out; clears `sink_timed_out` on success.

### Removed (dead-code waves)

- **#315**: Window operator unification deleted ≈13K LOC of DAG-only
  window operators.
- **#316**: Removed ≈23K LOC of dead DAG / checkpoint / storage paths
  (`core/dag/`, `core/state/`, `storage/checkpoint/`, `incremental/`,
  `wal.rs`). The single `StreamingCoordinator` tokio task is now the
  only execution path.
- **#320 / #326**: Removed ≈5K LOC from `laminar-db`, including the
  old `StreamExecutor` (4,652 LOC).

These deletions are not backwards-compatible. There were no public
APIs touched, but checkpoints from 0.20.x cannot be restored on 0.21+.

## [0.20.2]

### Added

- **Production-grade Prometheus metrics** (#339): expanded `/metrics`
  with pipeline counters, cycle duration percentiles, checkpoint
  telemetry, sink error/timeout counters, and per-connector gauges.
- **Temporal probe join DDL round-trip**: `CREATE STREAM` and
  `CREATE MATERIALIZED VIEW` now preserve the raw query text between
  `AS` and `EMIT` via a `query_sql` field on the AST, so custom
  streaming syntax (e.g. `TEMPORAL PROBE JOIN ... LIST (...)`,
  `RANGE FROM ... TO ... STEP ...`) survives the DDL parse / replay
  cycle used by `SHOW CREATE` and hot reload.

### Changed / Removed

- **Removed `WatermarkDynamicFilter`** (#338): the dead dynamic
  filter module was deleted. Watermark-based row filtering now runs
  through the standard streaming scan path.
- **Moved `examples/microstructure/pipeline.sql`** to use `TIMESTAMP`
  for the event-time column `"T"` (matches the v0.20.1 timestamp
  migration).
- **Deleted stale planning docs**: `docs/features/INDEX.md` and the
  `docs/plans/` directory were removed. `docs/ROADMAP.md` is the
  canonical feature tracker.

### Fixed

- **Watermark fallback wiring** (#337): `max.out.of.orderness.ms` is
  now respected when event-time extraction falls back to default
  generators. The attestation gate on the fallback path was removed.
- **Session window cardinality + CDC / sink shutdown** (#335):
  session windows cap per-key sessions to prevent unbounded state
  growth; sinks and CDC sources shut down cleanly when the pipeline
  stops.

## [0.20.1]

### Breaking: Timestamp column migration

Event-time columns throughout LaminarDB now use Arrow `Timestamp(_)`
instead of `Int64`-as-epoch-millis. The `interval_rewriter` is gone;
DataFusion handles `Timestamp ± INTERVAL` arithmetic natively.

**Schema changes in connector-produced columns:**
- OTel `_laminar_received_at`: `Int64` → `Timestamp(Nanosecond)`
- Postgres CDC `_ts_ms`: `Int64` → `Timestamp(Millisecond)`
- Kafka metadata `_timestamp`: `Int64` → `Timestamp(Millisecond)`
- MongoDB CDC `_wall_time_ms`: `Int64` → `Timestamp(Millisecond)`
- Files `_metadata.file_modification_time`: `Int64` → `Timestamp(Millisecond)`
- WebSocket `event.time.field` extraction: now reads any `Timestamp(_)`

**User-facing SQL changes:**
- Event-time DDL must declare `TIMESTAMP`, not `BIGINT`. Hand-written
  sources that used `ts BIGINT, WATERMARK FOR ts` need to switch to
  `ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL 'N' SECOND`.
- `INTERVAL` arithmetic is now native on `TIMESTAMP` columns.
  `ts BETWEEN t - INTERVAL '10' SECOND AND t + INTERVAL '10' SECOND`
  works. The old `t - 10000` numeric-arithmetic trick is no longer
  required.
- `tumble()` / `hop()` / `session()` UDFs accept any `Timestamp(_)`
  precision (Second / Millisecond / Microsecond / Nanosecond).
  Return type remains `Timestamp(Millisecond)`.

**Removed APIs:**
- `laminar_sql::parser::interval_rewriter` module
- `laminar_core::time::TimestampFormat` enum
- `EventTimeExtractor::from_column(name, format)`: now takes only `name`
- `laminar_db::db::infer_timestamp_format`
- `laminar_db::sql_analysis::infer_ts_format_from_batch`

**New APIs:**
- `laminar_core::time::cast_to_millis_array`: shared helper that
  normalises any `Timestamp(_)` array to `TimestampMillisecondArray`
  via Arrow's cast kernel. Used by the window UDFs, event-time
  extractor, interval-join key helper, and the WebSocket parser.

**Fixed:**
- `TUMBLE`/`HOP`/`SESSION` over `Timestamp(Nanosecond)` no longer
  errors with "Unsupported timestamp type" at runtime.
- Interval joins over `Timestamp(Nanosecond)` columns no longer
  error when extracting join keys.
- OTel `_laminar_received_at` unit-inference bug. Windows that
  used to be silently 1,000,000× smaller than declared (nanos
  treated as millis) now use real wall-clock durations.
- `WATERMARK FOR` in TOML config composes with connector schema
  auto-discovery. Columnless sources with a watermark clause are
  validated against the discovered schema instead of bailing out.

**Migration notes:**
- Existing checkpoints from v0.20.0 are **not** compatible. They
  reference Int64 timestamp columns in serialized operator state.
  Wipe `./data/checkpoints` before starting the server on v0.20.1.
- Hand-written SQL with `ts BIGINT` needs to change to `ts TIMESTAMP`
  if the column is used in a watermark or window. Plain `BIGINT`
  columns unrelated to event time are unchanged.

## [0.20.0] and earlier: notable additions

### Added: MongoDB CDC Source & Sink (`mongodb-cdc` feature, PR #255)

Landed in the 0.20.x line via PR #255. The feature provides MongoDB
change-stream source and write-sink connectors, CDC envelope types,
full-document modes, time-series sink configuration, and CDC replay writes.

The experimental `ResumeTokenStore` and `LargeEventReassembler` APIs later
proved unsafe or unwired and were removed. Resume tokens are no longer
persisted independently of the engine checkpoint, and split-large-event
handling is not advertised by the connector.
