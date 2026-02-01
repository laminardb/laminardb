#!/bin/bash
# Create Kafka topics for the ShopStream demo (using Redpanda's rpk).
# Run after docker-compose up:
#   bash examples/shopstream/scripts/setup-kafka.sh

set -e

CONTAINER="${CONTAINER:-shopstream-redpanda}"
PARTITIONS="${PARTITIONS:-3}"

echo "Creating ShopStream topics via rpk in $CONTAINER..."

# Input topics
for topic in clickstream orders inventory_updates; do
    echo "  Creating topic: $topic"
    docker exec "$CONTAINER" \
        rpk topic create "$topic" \
        --partitions "$PARTITIONS"
done

# Output topics
for topic in session-analytics sales-kpis fraud-alerts inventory-alerts \
             trending-products revenue-by-category; do
    echo "  Creating topic: $topic"
    docker exec "$CONTAINER" \
        rpk topic create "$topic" \
        --partitions "$PARTITIONS"
done

echo ""
echo "Done. Topics:"
docker exec "$CONTAINER" rpk topic list
