#!/usr/bin/env bash
# Create Kafka topics in Redpanda for the LaminarDB Market Data demo.
set -euo pipefail

CONTAINER="${REDPANDA_CONTAINER:-laminardb-demo-redpanda}"

echo "=== Creating topics in Redpanda ==="

docker exec "$CONTAINER" rpk topic create market-ticks \
    --partitions 5 \
    --config retention.ms=3600000

docker exec "$CONTAINER" rpk topic create order-events \
    --partitions 5 \
    --config retention.ms=3600000

docker exec "$CONTAINER" rpk topic create ohlc-bars \
    --partitions 5 \
    --config retention.ms=3600000

docker exec "$CONTAINER" rpk topic create volume-metrics \
    --partitions 5 \
    --config retention.ms=3600000

docker exec "$CONTAINER" rpk topic create anomaly-alerts \
    --partitions 1 \
    --config retention.ms=3600000

echo ""
echo "=== Topics created ==="
docker exec "$CONTAINER" rpk topic list
