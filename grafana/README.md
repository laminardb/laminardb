# LaminarDB Grafana Dashboards

There are two dashboards available:
1. **Single-Node/Overview Dashboard:** Import [laminardb.json](laminardb.json) into Grafana (Dashboards > Import > Upload JSON file).
2. **Cluster-Mode Dashboard:** Import [laminardb-cluster.json](laminardb-cluster.json) into Grafana for cluster-wide aggregates, per-node breakdowns, and distributed checkpoint metrics.


## Setup

1. Add a Prometheus datasource pointing at your Prometheus server
2. Configure Prometheus to scrape `http://<laminardb-host>:8080/metrics`
3. Import the dashboard and select the Prometheus datasource

Example `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: laminardb
    scrape_interval: 10s
    static_configs:
      - targets: ['localhost:8080']
```

## Layout

- **Pipeline Overview** — ingested/emitted/dropped totals, uptime, backpressure, WS connections
- **Throughput** — events/sec rates, cycle duration percentiles (p50/p99)
- **Checkpoints** — epoch, completed/failed counts, recent successful completions, size, duration percentiles
- **Sink Errors** — write failures, timeouts, channel closed rates, 2PC latency
- **Kafka Source** (collapsed) — reader offset lag, sample availability/age, source delivery age, poll rate, advisory commits, rebalances
- **Kafka Sink** (collapsed) — write rate, produce latency
- **PostgreSQL CDC** (collapsed) — replication lag, insert/update/delete rates
- **Delta Lake Sink** (collapsed) — commits, table version, rows flushed
- **MongoDB** (collapsed) — CDC event rates, sink write rates

Connector sections are collapsed by default — expand the ones relevant to your pipeline.

## Kafka progress

The overview dashboard also works for individual cluster nodes. Kafka progress metrics use a
`source` label; partition metrics also use `topic` and `partition`. Revoked partition series are
removed. The server adds the `laminardb_` prefix and `instance`/`pipeline` labels.
DB startup supplies the source name automatically. Direct `KafkaSource` callers must provide
`laminar.source.name` in the `SourceStart` configuration and a Prometheus registry to expose these
named metrics. Omitting either leaves connector operation unchanged.

| Metric (before the server prefix) | Meaning |
|---|---|
| `kafka_source_reader_lag_offsets` | Broker high watermark minus the next offset delivered to the Kafka reader. Offset distance, not processed rows or committed recovery lag; may include uncommitted transactions. |
| `kafka_source_lag_sample_available` | `1` for a usable sample, `0` when unavailable. Availability alone does not establish freshness. |
| `kafka_source_lag_sample_timestamp_seconds` | Time of the last successful lag sample; the series is absent before the first success. |
| `kafka_source_last_batch_timestamp_seconds` | Time of the last nonempty successful connector poll; `0` before the first batch. This measures connector delivery, not event-time freshness or sink visibility. |

Collection runs every 10 seconds with a bounded 10-second round and no overlapping rounds.
The lag panel hides unavailable samples and samples older than 30 seconds rather than displaying
them as zero. Separate availability and age panels expose those conditions. Timestamp age queries
require synchronized clocks; no sample means unknown, not healthy zero lag. An idle producer
legitimately increases source delivery age, so alert on that age only when continued input is expected.

For selected, assigned partitions, this condition identifies unavailable or stale telemetry:

```promql
(laminardb_kafka_source_lag_sample_available == 0)
or (time() - laminardb_kafka_source_lag_sample_timestamp_seconds > 30)
```

Scope alerts to the intended deployment and allow startup/collection grace. Missing series and
failed scrapes need separate no-data handling. Broker commit acknowledgements remain advisory;
LaminarDB recovery uses committed engine checkpoints. These panels do not establish settled SQL
progress or change `/ready` authority and lifecycle checks.

## Checkpoint progress alerts

The existing successful-completion counter can detect a lack of recent checkpoint progress; it
does not report the exact age of the last checkpoint. Keep instances separate so one healthy node
cannot hide a stalled participant. For example:

```yaml
- alert: LaminarDBCheckpointProgressStalled
  expr: increase(laminardb_checkpoints_completed_total{job="laminardb",pipeline="orders-prod"}[5m]) == 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "No recent successful LaminarDB checkpoint completion"
```

Replace the selectors and enable this only for pipelines expected to be running with periodic
checkpointing. Manual or disabled checkpoint schedules are not covered. Choose the window and
pending duration from the configured checkpoint interval, attempt/recovery budget and scrape
interval; five minutes is an example, not an SLO. Use existing readiness probes and maintenance
inhibition for lifecycle changes: `up == 1` proves scrape success, not Running state. Missing
series or too few samples are not zero completions and need separate monitoring. Counter resets
are handled by `increase`; its result is an estimate over observed samples.
