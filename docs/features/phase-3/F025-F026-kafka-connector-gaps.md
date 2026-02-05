# Kafka Connector Gap Analysis

> Generated: 2026-02-05
> Updated: 2026-02-05
> Based on: librdkafka 2.x, Apache Flink 1.18+, RisingWave, Kafka Connect

## Summary

| Category | Critical | Important | Nice-to-Have |
|----------|----------|-----------|--------------|
| Security | ~~2~~ **0** | ~~1~~ **0** | 1 |
| Consumer | ~~1~~ **0** | ~~4~~ **0** | 2 |
| Producer | 0 | ~~2~~ **1** | 2 |
| Serialization | 1 | ~~1~~ **0** | 2 |
| Streaming | 0 | 2 | 2 |
| **Total** | ~~4~~ **0** | ~~9~~ **3** | **9** |

### Phase 1 Complete (2026-02-05)

Implemented:
- SecurityProtocol enum (plaintext/ssl/sasl_plaintext/sasl_ssl)
- SaslMechanism enum (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/GSSAPI/OAUTHBEARER)
- IsolationLevel enum (read_uncommitted/read_committed)
- TopicSubscription enum (Topics/Pattern for regex)
- Explicit security fields (sasl_*, ssl_*)
- Fetch tuning fields (fetch_min_bytes, fetch_max_bytes, fetch_max_wait_ms, max_partition_fetch_bytes)
- batch_num_messages for producer
- include_headers for source
- schema_registry_ssl_ca_location
- 45+ new ConfigKeySpec documentation entries

Tests: 140 Kafka tests passing

### Phase 2 Complete (2026-02-05)

Implemented:
- StartupMode enum (GroupOffsets/Earliest/Latest/SpecificOffsets/Timestamp)
- startup.mode, startup.specific.offsets, startup.timestamp.ms config keys
- parse_specific_offsets() helper for "partition:offset,..." format
- schema_registry_ssl_certificate_location for SR client cert
- schema_registry_ssl_key_location for SR client key
- 12 new unit tests for StartupMode and SR SSL fields

Tests: 39 Kafka config tests, 567 total connector tests passing

---

## Critical Gaps (P0) - ✅ ALL COMPLETE

### 1. ✅ SecurityProtocol Enum - COMPLETE

Implemented in `config.rs`:
- `SecurityProtocol` enum with `Plaintext`, `Ssl`, `SaslPlaintext`, `SaslSsl`
- `as_rdkafka_str()`, `uses_ssl()`, `uses_sasl()` helper methods
- `FromStr` parsing with case-insensitive matching
- Added to both `KafkaSourceConfig` and `KafkaSinkConfig`
- Validation: SASL requires mechanism, SSL validates CA location

### 2. ✅ SaslMechanism Enum - COMPLETE

Implemented in `config.rs`:
- `SaslMechanism` enum with `Plain`, `ScramSha256`, `ScramSha512`, `Gssapi`, `Oauthbearer`
- `requires_credentials()` helper to check if username/password needed
- Added to both configs
- Validation: credential-based mechanisms require username/password

### 3. ✅ IsolationLevel Enum - COMPLETE

Implemented in `config.rs`:
- `IsolationLevel` enum with `ReadUncommitted`, `ReadCommitted` (default)
- Applied in `to_rdkafka_config()` for consumer
- ConfigKeySpec documentation added

### 4. ✅ Topic Regex Subscription - COMPLETE

Implemented in `config.rs`:
- `TopicSubscription` enum with `Topics(Vec<String>)` and `Pattern(String)`
- `topic.pattern` config key for regex subscriptions
- Source uses `^` prefix for rdkafka regex
- Validation: pattern cannot be empty
- `topics` field deprecated with migration path

---

## Important Gaps (P1) - Mostly Complete

### 5. ✅ StartupMode Enum - COMPLETE

Implemented in `config.rs`:
- `StartupMode` enum with `GroupOffsets`, `Earliest`, `Latest`, `SpecificOffsets(HashMap<i32, i64>)`, `Timestamp(i64)`
- `overrides_offset_reset()` and `as_offset_reset()` helper methods
- `startup.mode`, `startup.specific.offsets`, `startup.timestamp.ms` config keys
- `parse_specific_offsets()` helper for "partition:offset,..." format
- FromStr parsing with case-insensitive matching

### 6. ✅ Explicit Security Fields - COMPLETE (Phase 1)

Implemented in `config.rs` and `sink_config.rs`:
- `security_protocol: SecurityProtocol`
- `sasl_mechanism: Option<SaslMechanism>`
- `sasl_username: Option<String>`
- `sasl_password: Option<String>`
- `ssl_ca_location: Option<String>`
- `ssl_certificate_location: Option<String>`
- `ssl_key_location: Option<String>`
- `ssl_key_password: Option<String>`

### 7. ✅ Fetch Tuning Fields (Consumer) - COMPLETE (Phase 1)

Implemented in `config.rs`:
- `fetch_min_bytes: Option<i32>` (default: 1)
- `fetch_max_bytes: Option<i32>` (default: 52428800)
- `fetch_max_wait_ms: Option<i32>` (default: 500)
- `max_partition_fetch_bytes: Option<i32>` (default: 1048576)

### 8. ✅ Header Support - COMPLETE (Phase 1)

Implemented:
- `include_headers: bool` in source config
- ConfigKeySpec documentation added

### 9. ✅ Schema Registry SSL - COMPLETE

Implemented in `config.rs`:
- `schema_registry_ssl_ca_location: Option<String>`
- `schema_registry_ssl_certificate_location: Option<String>`
- `schema_registry_ssl_key_location: Option<String>`

### 10. Protobuf Deserializer

Currently declared but returns `Err(unimplemented)`.

### 11. Batch Tuning (Producer)

```rust
pub batch_size: Option<i32>,           // default: 16384
pub batch_num_messages: Option<i32>,   // default: 10000
pub queue_buffering_max_ms: Option<i32>, // linger.ms equivalent
```

### 12. Per-Partition Watermarks Integration

Connect F064 (Per-Partition Watermarks) to KafkaSource.

### 13. Watermark Alignment

Connect F066 (Watermark Alignment Groups) for multi-topic joins.

---

## Nice-to-Have Gaps (P2)

### 14. Socket Tuning

```rust
pub socket_timeout_ms: Option<i32>,
pub socket_keepalive_enable: Option<bool>,
pub socket_nagle_disable: Option<bool>,
```

### 15. Connection Tuning

```rust
pub connections_max_idle_ms: Option<i32>,
pub reconnect_backoff_ms: Option<i32>,
pub reconnect_backoff_max_ms: Option<i32>,
```

### 16. Consumer Heartbeat Tuning

```rust
pub heartbeat_interval_ms: Option<i32>,  // default: 3000
pub max_poll_interval_ms: Option<i32>,   // default: 300000
```

### 17. Consumer Statistics

```rust
pub statistics_interval_ms: Option<i32>,  // 0 = disabled
```

### 18. Producer Retry Tuning

```rust
pub message_timeout_ms: Option<i32>,
pub request_timeout_ms: Option<i32>,
pub delivery_timeout_ms: Option<i32>,
```

### 19. Idempotent Producer Mode

Explicit flag for idempotent without full transactions.

### 20. JSON Schema Registry

Support JSON Schema in addition to Avro.

### 21. Consumer Interceptors

Plugin points for metrics/logging.

### 22. Rate Limiting Integration

Connect F034 (Connector SDK) rate limiters.

---

## Implementation Order

### Phase 1: Critical ✅ COMPLETE
1. ✅ SecurityProtocol enum + fields
2. ✅ SaslMechanism enum + fields
3. ✅ IsolationLevel enum + field
4. ✅ TopicSubscription enum

### Phase 2: Security & Startup ✅ COMPLETE
5. ✅ Explicit security fields (ssl_*, sasl_*) - Done in Phase 1
6. ✅ StartupMode enum
7. ✅ Schema Registry SSL fields

### Phase 3: Tuning & Features ✅ MOSTLY COMPLETE
8. ✅ Fetch tuning fields - Done in Phase 1
9. ✅ Batch tuning fields - batch_num_messages done in Phase 1
10. ✅ Header support - include_headers done in Phase 1
11. Protobuf deserializer - Deferred

### Phase 4: Streaming Integration
12. Per-partition watermarks (F064)
13. Watermark alignment (F066)

### Phase 5: Polish
14-22. Nice-to-have improvements

---

## Testing Strategy

Each new enum/field requires:
1. Unit test for FromStr parsing
2. Unit test for as_rdkafka_str() conversion
3. Integration test for from_config() extraction
4. Validation test for invalid combinations
5. ConfigKeySpec documentation entry
