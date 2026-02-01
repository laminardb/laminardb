# F-DEMO-003: Kafka Integration & Docker

## Overview

| Field | Value |
|-------|-------|
| **ID** | F-DEMO-003 |
| **Title** | Kafka Integration & Docker |
| **Phase** | 3 (Connectors & Integration) |
| **Priority** | P1 |
| **Status** | Draft |

## Description

Optional Kafka mode with a separate producer binary and Docker Compose setup using Redpanda (Kafka-compatible, no JVM/Zookeeper).

## Components

### Producer Binary (`src/producer.rs`)
- Feature-gated behind `kafka` Cargo feature
- Generates synthetic market data as JSON
- Produces to Redpanda topics at configurable rate
- Standalone binary: `cargo run -p laminardb-demo --bin producer --features kafka`

### Docker Compose
- Redpanda v24.2.5 (Kafka-compatible broker)
- Redpanda Console (web UI for topic inspection)
- Ports: 9092 (Kafka API), 8080 (Console)

### Topics
| Topic | Key | Description |
|-------|-----|-------------|
| market-ticks | symbol | Raw market tick events |
| order-events | symbol | Order fill events |

### Setup Script
`scripts/setup-kafka.sh` - Creates topics via `rpk` CLI inside Redpanda container.

## Running

```bash
# Start infrastructure
docker-compose -f examples/demo/docker-compose.yml up -d
bash examples/demo/scripts/setup-kafka.sh

# Start producer
cargo run -p laminardb-demo --bin producer --features kafka

# Start consumer (in another terminal)
DEMO_MODE=kafka cargo run -p laminardb-demo --features kafka
```

## Files

- `src/producer.rs` - Kafka producer binary
- `docker-compose.yml` - Redpanda + Console
- `scripts/setup-kafka.sh` - Topic creation
- `sql/sources_kafka.sql` - CREATE SOURCE FROM KAFKA
- `sql/sinks_kafka.sql` - CREATE SINK INTO KAFKA
