-- Kafka sinks for the Market Data demo.
-- Writes analytics results to Redpanda/Kafka topics.

CREATE SINK ohlc_output FROM ohlc_bars
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'ohlc-bars'
) FORMAT JSON;

CREATE SINK volume_output FROM volume_metrics
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'volume-metrics'
) FORMAT JSON;

CREATE SINK anomaly_output FROM anomaly_alerts
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'anomaly-alerts'
) FORMAT JSON;

CREATE SINK imbalance_output FROM book_imbalance
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'book-imbalance'
) FORMAT JSON;

CREATE SINK spread_output FROM spread_metrics
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'spread-metrics'
) FORMAT JSON;

CREATE SINK depth_output FROM depth_metrics
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'depth-metrics'
) FORMAT JSON;
