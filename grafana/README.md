# LaminarDB Grafana Dashboard

Import `laminardb.json` into Grafana (Dashboards > Import > Upload JSON file).

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
- **Checkpoints** — epoch, completed/failed counts, size, duration percentiles
- **Sink Errors** — write failures, timeouts, channel closed rates, 2PC latency
- **Kafka Source** (collapsed) — consumer lag, poll rate, commits, rebalances
- **Kafka Sink** (collapsed) — write rate, produce latency
- **PostgreSQL CDC** (collapsed) — replication lag, insert/update/delete rates
- **Delta Lake Sink** (collapsed) — commits, table version, rows flushed, compaction
- **MySQL CDC** (collapsed) — binlog position, event rates
- **MongoDB** (collapsed) — CDC event rates, sink write rates

Connector sections are collapsed by default — expand the ones relevant to your pipeline.
