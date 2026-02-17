# Constellation Feature Dependency Graph

## Phase 6a — Partition-Parallel Embedded

### State Store Chain

```
F-STATE-001 (StateStore Trait)
      │
      ├──▶ F-STATE-002 (InMemoryStateStore)
      │
      ├──▶ F-STATE-003 (MmapStateStore) ──▶ F-STATE-004 (Pluggable Snapshots)
      │                                            │
      │                                            ├──▶ F-DCKP-007 (Incremental Mmap) [Phase 6c]
      │                                            └──▶ F-PERF-001 (State Benchmarks)
      │
      ├──▶ F-DCKP-004 (ObjectStoreCheckpointer)
      │
      └──▶ F-LOOKUP-001 (LookupTable Trait)
```

### Checkpoint Chain (Embedded)

```
F-STREAM-002 (SPSC Channel) [existing]
      │
      └──▶ F-DCKP-001 (Barrier Protocol)
                  │
                  ├──▶ F-DCKP-002 (Barrier Alignment) ──▶ F-DCKP-006 (Unaligned) [Phase 6c]
                  │
                  ├──▶ F-DCKP-008 (Distributed Coordination) [Phase 6b]
                  │
                  └──▶ F-E2E-001 (Source Offset Checkpoint)

F-DCKP-003 (Object Store Layout)
      │
      └──▶ F-DCKP-004 (ObjectStoreCheckpointer)
                  │
                  ├──▶ F-DCKP-005 (Recovery Manager)
                  │
                  └──▶ F-PERF-003 (Checkpoint Cycle Benchmark)
```

### Lookup Table Chain

```
F-LOOKUP-003 (Predicate Types) ──┐
                                 │
F-LOOKUP-001 (LookupTable Trait) ├──▶ F-LOOKUP-002 (LookupSource Trait)
                                 │           │
                                 │           ├──▶ F-LOOKUP-007 (PostgresLookupSource)
                                 │           └──▶ F-LOOKUP-008 (ParquetLookupSource)
                                 │
                                 ├──▶ F-LOOKUP-004 (foyer Memory Cache)
                                 │           │
                                 │           ├──▶ F-LOOKUP-005 (foyer Hybrid Cache)
                                 │           └──▶ F-PERF-002 (Cache Benchmarks)
                                 │
                                 └──▶ F-LOOKUP-006 (CDC-to-Cache Adapter)
                                             │
                                             └── requires F027 (PostgreSQL CDC) [existing]

F006B (SQL Parser) [existing]
      │
      └──▶ F-LSQL-001 (CREATE LOOKUP TABLE)
                  │
                  └──▶ F-LSQL-002 (Lookup Join Plan Node)
                              │
                              └──▶ F-LSQL-003 (Predicate Splitting)
                                          │
                                          └── requires F-LOOKUP-003
```

### Cross-Partition & Exactly-Once

```
F013 (Thread-Per-Core) [existing]
      │
      └──▶ F-XAGG-001 (Cross-Partition HashMap)

F-DCKP-003 + F-DCKP-004
      │
      └──▶ F-E2E-001 (Source Offset Checkpoint)
```

---

## Phase 6b — Constellation Foundation

### Discovery & Coordination

```
F-DISC-001 (Discovery Trait + Static)
      │
      ├──▶ F-DISC-002 (Gossip Discovery / chitchat)
      │           │
      │           ├──▶ F-EPOCH-003 (Reassignment Protocol)
      │           └──▶ F-XAGG-002 (Gossip Aggregates)
      │
      └──▶ F-DISC-003 (Kafka Group Discovery)
                  │
                  └── requires F025 (Kafka Source) [existing]

F-DISC-001
      │
      └──▶ F-COORD-001 (Raft Metadata / openraft)
                  │
                  ├──▶ F-EPOCH-001 (PartitionGuard + Epoch Fencing)
                  │           │
                  │           └──▶ F-EPOCH-002 (Assignment Algorithm)
                  │
                  ├──▶ F-EPOCH-003 (Reassignment Protocol)
                  │
                  └──▶ F-COORD-002 (Constellation Orchestration)
```

### Distributed Checkpoint & RPC

```
F-RPC-001 (gRPC Service Definitions)
      │
      ├──▶ F-RPC-002 (Remote Lookup Service)
      │           │
      │           └──▶ F-LOOKUP-009 (Partitioned Lookup Strategy)
      │
      ├──▶ F-RPC-003 (Barrier Forwarding)
      │           │
      │           └──▶ F-DCKP-008 (Distributed Checkpoint Coordination)
      │
      └──▶ F-XAGG-003 (gRPC Aggregate Fan-Out)
```

### Partition Ownership

```
F-COORD-001 (Raft) + F-DISC-002 (Gossip)
      │
      ├──▶ F-EPOCH-001 (PartitionGuard)
      │
      ├──▶ F-EPOCH-002 (Assignment Algorithm)
      │
      └──▶ F-EPOCH-003 (Reassignment Protocol)
                  │
                  └──▶ F-PERF-004 (Constellation Checkpoint Benchmark)
```

---

## Phase 6c — Production Hardening

### Advanced Checkpointing

```
F-DCKP-002 (Barrier Alignment)
      │
      └──▶ F-DCKP-006 (Unaligned Checkpoints)

F-STATE-003 (Mmap) + F-STATE-004 (Snapshots)
      │
      └──▶ F-DCKP-007 (Incremental Mmap Checkpoints)
```

### Exactly-Once Sinks

```
F-DCKP-001 (Barriers)
      │
      ├──▶ F-E2E-002 (Transactional Sink / 2PC)
      │           │
      │           └── requires F026 (Kafka Sink), F027B (Postgres Sink) [existing]
      │
      └──▶ F-E2E-003 (Idempotent Sink)
```

### Server

```
F-SERVER-001 (TOML Config)
      │
      └──▶ F-SERVER-002 (Engine Construction)
                  │
                  ├──▶ F-SERVER-003 (HTTP API)
                  │           │
                  │           └──▶ F-SERVER-004 (Hot Reload)
                  │
                  └──▶ F-SERVER-005 (Constellation Mode)
                              │
                              └──▶ F-SERVER-006 (Rolling Restart)
                                          │
                                          └── requires F-EPOCH-003
```

---

## Critical Path

The minimum set of features required for a working Tier 3 constellation:

```
Phase 6a (Embedded Foundation):
  F-STATE-001 → F-STATE-002 → F-DCKP-001 → F-DCKP-002 → F-DCKP-003 →
  F-DCKP-004 → F-DCKP-005 → F-E2E-001

Phase 6b (Constellation):
  F-DISC-001 → F-DISC-002 → F-COORD-001 → F-EPOCH-001 → F-EPOCH-003 →
  F-RPC-001 → F-RPC-003 → F-DCKP-008

Phase 6c (Server):
  F-SERVER-001 → F-SERVER-002 → F-SERVER-003 → F-SERVER-005
```

Lookup tables (F-LOOKUP-*) and cross-partition aggregation (F-XAGG-*) can be developed in parallel with the critical path.
