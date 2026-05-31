# LaminarDB Agent Knowledge & Context Database

This document serves as the primary, harness-neutral repository of agent knowledge for the LaminarDB codebase. It contains the engineering context, architectural decisions, and current progress status. Any AI assistant or developer working on the codebase should consult this file as the single source of truth.

---

## 1. System Overview & Active State

* **Target Branch**: `feat/lookup-source-production` (implementing streaming cluster primitives)
* **Goal**: Build a production-grade, low-latency, distributed streaming cluster mode (similar to Apache Flink, RisingWave, or Arroyo) with robust operational guarantees.
* **Workspace Structure**:
  - `crates/laminar-core`: Core engine logic (state storage, shuffling, checkpoints, cluster coordination).
  - `crates/laminar-db`: Database shell, query processing, and node management.
  - `crates/laminar-sql`: Repartition and query planning extensions for DataFusion.
  - `crates/laminar-server`: Server entrypoints.
  - `crates/laminar-connectors`: System ingestion and egress connectors.

---

## 2. Principal Architecture Review & "AI Slop" Findings

The following issues were identified during code review of the legacy cluster implementation:

1. **Gossip Eventual Consistency for ACID Barrier Synchronization**:
   - The checkpoint barrier protocol (Prepare/Ack/Commit/Abort) ran on top of a UDP gossip membership layer (`Chitchat`).
   - Gossip is eventually consistent and slow; using it for synchronous barrier alignment introduced hundreds of milliseconds of latency jitter.
   - *Refactor target*: Replace with direct gRPC leader-follower RPCs.
2. **Busy-Wait Polling and Lock Contention**:
   - The leader polled follower acknowledgments by scanning gossip KV every 50ms, causing high CPU usage and lock contention on the global node state.
   - *Refactor target*: Event-driven gRPC calls.
3. **S3/Object Store Polling (Ticker-based)**:
   - Worker nodes polled the cloud object store every 2 seconds via `store.load()` to detect topology and vnode changes, creating high API costs.
   - *Refactor target*: Push topology changes reactively from the control plane.
4. **Row-Shuffle Routing Overhead**:
   - Row-shuffle hashed individual rows and copied them into sub-batches via `arrow::compute::take`, creating a CPU and memory allocation bottleneck.
   - *Refactor target*: Vectorized routing grouping batches by node rather than single rows.
5. **Weak Leader Election**:
   - Heuristic "lowest-ID wins" election over gossip is vulnerable to split-brain.
   - *Refactor target*: Standardize on single authoritative coordinator (or eventual Raft).
6. **Hand-Rolled TCP Shuffle Transport**:
   - Network shuffling of Arrow batches was built on raw TCP framing and custom sockets.
   - *Refactor target*: Refactor to client-streaming Tonic gRPC.
7. **Consistent Hashing Over-Engineering in Embedded Mode**:
   - The default mode split state into 256 virtual nodes (vnodes) writing 256 files per checkpoint, causing high cloud storage API latency and costs.
   - *Refactor target*: Make the vnode count configurable to `1` for single-node / embedded mode.
8. **JSONB Row-by-Row Serialization**:
   - JSON was packed into a custom binary format inside Arrow `LargeBinary` columns and processed row-by-row with custom UDFs, bypassing DataFusion vectorization.
   - *Refactor target*: Ingest JSON directly into native nested Arrow arrays (`StructArray`, `MapArray`).
9. **AI Slop & Redundant Code**:
   - Repetitive boilerplate trait implementations (e.g. `Eq`, `Hash` on UDFs) and verbose comments explaining basic language features.
   - *Refactor target*: Derive traits and keep comments focused only on architecture or safety invariants.

---

## 3. Tonic gRPC Setup & Code Generation

To support distributed clustering, gRPC services are gated under the `cluster-unstable` Cargo feature flag. This ensures the default build compiles cleanly without requiring the `protoc` compiler.

### Proto Schemas (`crates/laminar-core/proto/`)
- **[shuffle.proto](file:///C:/Users/sujit/source/laminardb/crates/laminar-core/proto/shuffle.proto)**:
  - Defines the client-streaming RPC `ShuffleTransport::Shuffle` to send `ShuffleFrame` payloads.
  - Payloads include a `Hello` handshake, `VnodeData` (Arrow IPC batch bytes), `Barrier` (epoch sync markers), and `Close`.
- **[barrier.proto](file:///C:/Users/sujit/source/laminardb/crates/laminar-core/proto/barrier.proto)**:
  - Defines the `BarrierSync` service with direct RPC calls from leader to follower:
    - `Prepare(PrepareRequest) -> Ack`
    - `Commit(CommitRequest) -> Ack`
    - `Abort(AbortRequest) -> Ack`

### Build Configuration
- Code generation is handled in **[build.rs](file:///C:/Users/sujit/source/laminardb/crates/laminar-core/build.rs)** using `tonic-prost-build`.
- Code generation compiles only when `CARGO_FEATURE_CLUSTER_UNSTABLE` is set (meaning `--features cluster-unstable` is active).

---

## 4. Completed Work

### Tonic gRPC Shuffle Refactor (Subtask 3)
- Fully refactored **[transport.rs](file:///C:/Users/sujit/source/laminardb/crates/laminar-core/src/shuffle/transport.rs)** to replace the raw TCP framing with the gRPC `ShuffleTransport` service.
- **Receiver (Server)**:
  - Listens on `local_addr`, runs a Tonic server, parses stream frames via a futures stream accept loop.
  - Feeds decoded record batches into a bounded `crossfire::mpsc::bounded_async` queue (`SHUFFLE_RECV_QUEUE`).
  - Implements the necessary thread-safety (`Send` + `Sync`) using `parking_lot::Mutex` and `tokio::Notify` wrappers to satisfy DataFusion plan constraints while using `crossfire` (which is `!Sync`).
- **Sender (Client)**:
  - Connects to peers lazily and maintains connections.
  - Drains a per-peer queue to stream frames via the client-streaming `shuffle` RPC.
  - Automatically reconnects by dropping the peer client on connection or transport errors.
- **Compatibility**:
  - Gated behind `#[cfg(feature = "cluster-unstable")]`.
  - When compiled without the feature, a networking-free shim with an identical public API is exposed to avoid breaking `laminar-db` or `laminar-server`.

---

## 5. Next Steps & Implementation Guidelines

The remaining work in the cluster migration plan consists of two main subtasks:

### Subtask 4: Refactor Barrier Synchronization
- **File**: `crates/laminar-core/src/cluster/control/barrier.rs`
- **Objective**: Replace the gossip-based barrier coordinate/observe/ack logic with direct gRPC client calls.
- **Technical Steps**:
  1. Gate gRPC barrier synchronization code under `#[cfg(feature = "cluster-unstable")]` using the generated `BarrierSyncClient` and `BarrierSyncServer` stubs.
  2. Implement the `BarrierSync` gRPC service on the follower. When a follower receives a `Prepare`/`Commit`/`Abort` request, trigger the corresponding local checkpoint operation and return an `Ack` message.
  3. Update `BarrierCoordinator` on the leader to store client connections to known followers.
  4. In `wait_for_quorum`, instead of loop-polling the gossip KV, the leader must dispatch gRPC requests (`Prepare`, `Commit`, or `Abort`) to all expected followers in parallel (e.g. using `futures::future::join_all`).
  5. Fall back to a local/noop or loopback implementation when compiling without `cluster-unstable` to preserve the default single-node execution mode.

### Subtask 5: Verification & Testing
- **Compilation check**:
  - Validate default build: `cargo check -p laminar-core`
  - Validate cluster build: `cargo check -p laminar-core --features cluster-unstable`
  - Ensure zero clippy warnings: `cargo clippy --all-targets --features cluster-unstable -- -D warnings`
- **Integration testing**:
  - Run tests with `cargo test --features cluster-unstable` (e.g., test the gRPC shuffle and the new gRPC barrier synchronization).

---

## 6. Guidelines & Conventions

- **Low-Latency Channels**: Use the `crossfire` crate rather than standard library or raw `tokio` channels for data plane operations.
- **Panic/Unwrap Hygiene**: Do not use `unwrap()`, `expect()`, or `panic!` inside core engine pathways. Propagate errors cleanly using `Result` and `thiserror`.
- **Safety Documentation**: Any `unsafe` blocks must have a `// SAFETY:` explanation.
- **Aesthetic Comments**: Avoid paragraph-long comments explaining standard Rust logic. Keep comments high-level, explaining architectural context and invariant guarantees.
