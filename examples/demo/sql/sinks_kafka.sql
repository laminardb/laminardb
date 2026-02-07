-- Kafka sinks for the Market Data demo.
-- Writes analytics results to Redpanda/Kafka topics.

CREATE SINK ohlc_output FROM ohlc_bars
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'ohlc-bars',
    format = 'json'
);

CREATE SINK volume_output FROM volume_metrics
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'volume-metrics',
    format = 'json'
);

CREATE SINK anomaly_output FROM anomaly_alerts
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'anomaly-alerts',
    format = 'json'
);

CREATE SINK imbalance_output FROM book_imbalance
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'book-imbalance',
    format = 'json'
);

CREATE SINK spread_output FROM spread_metrics
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'spread-metrics',
    format = 'json'
);

CREATE SINK depth_output FROM depth_metrics
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'depth-metrics',
    format = 'json'
);
