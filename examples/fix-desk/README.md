# FIX drop-copy desk

A trading desk built from a FIX drop copy with SQL only. LaminarDB logs on to the counterparty's
FIX session, journals every ExecutionReport, and maintains:

| View | What it answers |
|---|---|
| `positions` | Net position, net cash, and turnover per account and instrument, live |
| `flow_1m` | Orders, fills, cancels, rejects, volume, and notional per instrument per minute |
| `rejects_1m` | Risk-check rejects per account per minute |
| `large_fills` | Every fill worth 1,000,000 or more |
| `order_bursts` | Ten or more orders from one account in one instrument, less than a second apart |
| `latency_1m` | Drop-copy delay: the venue's TransactTime to the moment the report was journaled |
| `feed_health` | Sequence numbers, reports recovered by resend, and possible gaps |
| `audit_trail` (sink) | Every report, exactly once, in a Delta Lake table |

`reconcile.sql` then checks the audit trail against what the venue says it sent, in DuckDB.

Everything venue-specific lives in one file per counterparty under `venues/`: endpoint,
credentials, dictionary, and which FIX tag feeds which column. `desk.sql` reads only those
columns, so the same desk runs unchanged on any FIX 4.4 or FIX 5.0 drop copy. A new venue, a
custom tag, or a new message type is a dictionary entry and a column mapping, not code.

| Venue file | Counterparty |
|---|---|
| `venues/simulator.sql` | The local broker simulator below |
| `venues/broker.sql` | Template for a broker or exchange drop copy over TLS with Username/Password |
| `venues/binance-testnet.sql` | Binance Spot testnet drop copy (Ed25519-signed Logon) |

## What is real and what is simulated

The FIX session is real on both sides: two QuickFIX engines over TCP, with Logon, Heartbeats,
sequence numbers, gap detection, and resends, validated against QuickFIX's standard FIX 4.4
dictionary. The simulator's order flow is synthetic: four accounts trading eight US equities
at illustrative prices, with partial fills, amendments, cancels, risk rejects, algorithmic
bursts, and occasional oversized orders. It is not a venue profile.

## Run it

Requirements: Rust, CMake and a C++17 compiler (for the FIX engine), Python 3 (`watch.py` uses
the standard library only), the DuckDB CLI (for `reconcile.sql`), curl, and bash (Git Bash on
Windows).

```bash
cargo build --release -p laminar-server --no-default-features --features fix,delta-lake

cd examples/fix-desk
./run.sh venue       # terminal 1: the broker simulator on port 9878
./run.sh server      # terminal 2: LaminarDB on venues/simulator.sql + desk.sql
./run.sh watch       # terminal 3: the desk, refreshed every second
```

The first run downloads QuickFIX's `FIX44.xml` (pinned by commit and SHA-256) into
`dictionaries/`. All state lives in `data/`; `./run.sh reset` deletes it. Changing the SQL
needs a reset, because checkpoints are bound to the pipeline definition.

Queries also work directly, for example
`curl -s localhost:8080/api/v1/sql -H 'Content-Type: application/json' -d '{"sql":"SELECT * FROM positions"}'`.

### Take the desk down mid-session

1. Kill the server hard while the simulator keeps trading: `pkill -9 laminardb`, or
   `Stop-Process -Name laminardb -Force` on Windows. The simulator reports the client as down
   and keeps queuing ExecutionReports.
2. Start it again with `./run.sh server`. LaminarDB restores the last checkpoint, replays its
   journal, and asks the venue to resend the rest. `feed_health.resent` counts the reports that
   arrived by resend, and `latency_1m` shows the outage as a spike in `max_ms`.
3. Stop the simulator (Ctrl-C), wait a few seconds for a checkpoint, and reconcile:

```
$ ./run.sh reconcile
┌────────────┬────────────┬────────────┬─────────┬────────────┐
│ venue_sent │ audit_rows │ duplicates │ missing │ unexpected │
├────────────┼────────────┼────────────┼─────────┼────────────┤
│      13188 │      13188 │          0 │       0 │          0 │
└────────────┴────────────┴────────────┴─────────┴────────────┘
```

That run (Windows 11 laptop, release build, 20 orders per second) killed the server after 90
seconds and restarted it 20 seconds later; 1,622 reports arrived by resend. Zero duplicates and zero missing reports through a hard kill is the exactly-once guarantee of a
single-node server: the FIX journal is the replayable source and the Delta Lake sink commits
with the checkpoint. See [docs/FIX_SOURCE.md](../../docs/FIX_SOURCE.md) for the failure model.

## Bring your own counterparty

1. Copy `venues/broker.sql` to `venues/<name>.sql`.
2. Point `dictionary.path` at the dictionary your counterparty publishes. Its custom fields,
   messages, and enumerations are what the engine validates against.
3. Adjust the `column.*` mappings if a field lives elsewhere. Columns may read the header
   (`header.56`), repeating groups (`453.448`), or metadata (`@seq_num`).
4. Keep the column names and types: `desk.sql` depends only on them.
5. Set the referenced environment variables (secrets only as `${VAR}`) and run
   `./run.sh server <name>`.

To add a venue-specific field to the desk, define it in the dictionary, add a column with
`'column.<name>' = '<tag>'`, and use it in SQL. The simulator can emit such fields with
`--field TAG=VALUE|VALUE`.

The desk is written for FIX 4.4 and later. FIX 4.2 reports trades with ExecType 1 and 2 instead
of F; adjust the `exec_type` filters for such a venue.

### Binance Spot testnet

`venues/binance-testnet.sql` connects to a real exchange's drop copy. It needs:

- an Ed25519 API key on the Spot testnet with the `FIX_API_READ_ONLY` or `FIX_API`
  permission (`BINANCE_API_KEY`) and its PEM private key (`BINANCE_KEY_FILE`);
- Binance's `spot-fix-oe.xml` dictionary from their API documentation repository
  (`BINANCE_DICTIONARY`);
- orders placed on the testnet account, because a drop copy only reports the account's own
  executions.

Binance does not resend: reports sent while the session is down are lost, and the first report
after every reconnect has `_fix_gap_before = true` (`feed_health.possible_gaps`).
