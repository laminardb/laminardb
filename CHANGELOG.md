# Changelog

## [Unreleased]

### Added — MongoDB CDC Source & Sink (`mongodb-cdc` feature)

New public API symbols in `laminar_connectors::mongodb`:

**Source Connector**
- `MongoDbCdcSource` — `SourceConnector` impl for MongoDB change streams
- `MongoDbSourceConfig` — connection, pipeline, full document mode config
- `FullDocumentMode` — Delta / UpdateLookup / RequirePostImage / WhenAvailable
- `mongodb_cdc_envelope_schema()` — Arrow schema for CDC envelope records

**Sink Connector**
- `MongoDbSink` — `SinkConnector` impl for MongoDB writes
- `MongoDbSinkConfig` — connection, batching, write concern config
- `WriteMode` — Insert / Upsert / Replace / CdcReplay
- `WriteConcernConfig`, `WriteConcernLevel` — write durability settings

**Change Events**
- `MongoDbChangeEvent` — typed change stream event
- `OperationType` — Insert / Update / Replace / Delete / Drop / Rename / Invalidate
- `UpdateDescription` — delta fields for update events
- `Namespace` — database + collection pair
- `TruncatedArray` — truncated array metadata (MongoDB 6.0+)

**Resume Token Persistence**
- `ResumeTokenStore` trait — pluggable async persistence
- `ResumeToken` — opaque resume token wrapper
- `FileResumeTokenStore` — file-based persistence (embedded/test)
- `MongoResumeTokenStore` — MongoDB-backed persistence (production, feature-gated)
- `InMemoryResumeTokenStore` — in-memory persistence (testing)
- `ResumeTokenStoreConfig` — configuration enum (File / Mongo / Memory)

**Time Series Support**
- `CollectionKind` — Standard / TimeSeries
- `TimeSeriesConfig` — time field, meta field, granularity, TTL
- `TimeSeriesGranularity` — Seconds / Minutes / Hours / Custom

**Large Event Handling**
- `LargeEventReassembler` — fragment reassembly for split events (MongoDB ≥ 6.0.9)
- `EventFragment`, `SplitEventInfo` — fragment types

**Write Model**
- `MongoWriteOp` — InsertOne / ReplaceOne / UpdateOne / DeleteOne / Lifecycle
- `build_update_document()` — $set/$unset update document builder
- `cdc_replay_op()` — operation type to write op translation

**Metrics**
- `MongoSourceMetrics` — lock-free source connector metrics
- `MongoSinkMetrics` — lock-free sink connector metrics

**Registry**
- `register_mongodb_cdc()` — registers source with ConnectorRegistry
- `register_mongodb_sink()` — registers sink with ConnectorRegistry
