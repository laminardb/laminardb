# Reactive Push-Based Subscriptions

> In-memory reactive subscription system for LaminarDB.
> See [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md) for background.

## Overview

Transforms LaminarDB's poll-based `subscribe()` API into a reactive push model where
data changes are automatically delivered to consumers with sub-microsecond latency.

**Key Design Principles**:
- Three-tier architecture: Ring 0 notification, Ring 1 dispatch, Ring 2 lifecycle
- Notification/data separation: Ring 0 pushes lightweight sequence numbers, consumer pulls data via zero-copy
- Hybrid push-pull: Push notifications with backpressure (Reactive Streams pattern)
- Three API styles: channel, callback, async Stream

## Architecture

```
Ring 0 (Hot Path)                Ring 1 (Background)           Ring 2 (Control)
+-----------------------+        +-----------------------+      +-------------------+
| NotificationSlot      |        | SubscriptionDispatcher|      | SubscriptionRegistry
| (AtomicU64 per MV)   |------->| (routes to channels)  |      | (lifecycle mgmt)  |
|                       |  SPSC  |                       |      |                   |
| Zero-allocation       |  queue | broadcast::Sender per |      | create/pause/     |
| <100ns notify         |        | query/MV              |      | resume/cancel     |
+-----------------------+        +-----------+-----------+      +-------------------+
                                             |
                              +--------------+--------------+
                              |              |              |
                        +-----v----+   +-----v----+   +----v-----+
                        | Channel  |   | Callback |   |  Stream  |
                        | Receiver |   | Handler  |   |  Adapter |
                        +----------+   +----------+   +----------+
                         F-SUB-005      F-SUB-006      F-SUB-007
```

## Features

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-SUB-001 | ChangeEvent Types | P0 | 📝 Draft | [Link](F-SUB-001-change-event-types.md) |
| F-SUB-002 | Notification Slot (Ring 0) | P0 | 📝 Draft | [Link](F-SUB-002-notification-slot.md) |
| F-SUB-003 | Subscription Registry | P0 | 📝 Draft | [Link](F-SUB-003-subscription-registry.md) |
| F-SUB-004 | Subscription Dispatcher (Ring 1) | P0 | 📝 Draft | [Link](F-SUB-004-subscription-dispatcher.md) |
| F-SUB-005 | Push Subscription API | P0 | 📝 Draft | [Link](F-SUB-005-push-subscription-api.md) |
| F-SUB-006 | Callback Subscriptions | P1 | 📝 Draft | [Link](F-SUB-006-callback-subscriptions.md) |
| F-SUB-007 | Stream Subscriptions | P1 | 📝 Draft | [Link](F-SUB-007-stream-subscriptions.md) |
| F-SUB-008 | Backpressure & Filtering | P1 | 📝 Draft | [Link](F-SUB-008-backpressure-filtering.md) |

## Dependency Graph

```
F-SUB-001 (ChangeEvent Types)
    |
    +---> F-SUB-002 (Notification Slot)
    |         |
    |         +---> F-SUB-004 (Dispatcher)
    |                    |
    +---> F-SUB-003 (Registry)
    |         |          |
    |         +----------+
    |                    |
    |              +-----+------+------+
    |              |            |      |
    |         F-SUB-005   F-SUB-006  F-SUB-007
    |         (Channel)   (Callback) (Stream)
    |              |            |      |
    |              +-----+------+------+
    |                    |
    +---> F-SUB-008 (Backpressure & Filtering)
```

## Implementation Order

1. **Phase 1: Core Infrastructure** - F-SUB-001, F-SUB-002, F-SUB-003
2. **Phase 2: Dispatch Layer** - F-SUB-004
3. **Phase 3: API Layer** - F-SUB-005, F-SUB-006, F-SUB-007
4. **Phase 4: Optimization** - F-SUB-008

## Latency Budget

| Component | Target | Ring |
|-----------|--------|------|
| Ring 0 notification (sequence increment) | < 100ns | 0 |
| SPSC queue to Ring 1 | < 200ns | 0 -> 1 |
| Dispatcher route + broadcast send | < 500ns | 1 |
| Total: notify to subscriber channel | < 1us | End-to-end |
| Subscriber callback invocation | < 2us | 2 |

## Success Criteria

- [ ] All 8 feature specifications created
- [ ] Notification latency < 1us from Ring 0 to subscriber
- [ ] Throughput > 10M events/sec with batching
- [ ] Zero allocations in Ring 0 notification path
- [ ] Slow subscribers never block Ring 0
- [ ] All three API styles (channel, callback, stream) functional
- [ ] 100+ tests across all features
- [ ] Benchmarks meeting latency and throughput targets

## References

- [Reactive Subscriptions Research 2026](../../../research/reactive-subscriptions-research-2026.md)
- [F-STREAM-006: Subscription](../streaming/F-STREAM-006-subscription.md) (existing poll-based API)
- [F-STREAM-010: Broadcast Channel](../streaming/F-STREAM-010-broadcast-channel.md)
- [F063: Changelog/Retraction](../../phase-2/F063-changelog-retraction.md) (Z-set foundation)
