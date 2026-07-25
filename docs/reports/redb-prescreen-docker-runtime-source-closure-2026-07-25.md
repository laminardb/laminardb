# redb prescreen Docker/runtime source closure — Cycle 34

- **Date:** 2026-07-25
- **Evidence class:** tagged-source and official-interface review only
- **Scope:** `D20.3`, `D20.4`, and the exact Engine schedule for a future redb-free dummy probe
- **Tuple under review:** rootful Linux/amd64 Moby 29.6.2, containerd 2.2.6, its matching
  `containerd-shim-runc-v2`, runc 1.3.6, Engine API v1.55
- **Configuration direction:** classic `vfs` graphdriver with containerd snapshotter/migration,
  networking, CDI, NRI, and background builder GC disabled
- **Executable, image, target, configuration, or mechanism selected:** none
- **Binary download, daemon/container, WSL, provider, candidate, backend, or soak execution:** none
- **Production verdict:** **NO-GO**

## Result

The Cycle 33 statement that dockerd has one relevant external containerd client was incomplete.
Moby constructs BuildKit even when no build route is admitted. With Docker 29's containerd image
store, the BuildKit controller takes a second external containerd client. That default path is
therefore outside the current fixed-connection hypothesis.

One narrower source-derived path remains worth a redb-free mechanism probe: explicitly disable
both `containerd-snapshotter` and `containerd-migration`, select the classic `vfs` graphdriver, and
disable builder garbage collection. In that branch BuildKit uses Moby's local graphdriver/content
adapter rather than another external containerd client. This narrows the expected graph; it does
not prove physical socket cardinality, initialization success, absence of lazy connections, or
compatibility with an externally supervised containerd. Any observed second external client,
reconnect, unexpected helper, plugin, network edge, or background transition rejects the tuple.

The source audit also expands the legitimate process set. Dockerd runs `runc features` twice during
startup, and each planned `/info` and `/version` request runs both `runc --version` and the configured
`docker-init --version` while also issuing a containerd Version RPC. BuildKit platform discovery
materializes and attempts one native amd64 payload plus ten non-amd64 payloads. The native payload
executes and its exit status encodes the host's amd64 ISA level; the `386` probe can execute through
native compatibility, while others may enter a registered `binfmt_misc` interpreter. Dockerd can
also invoke `apparmor_parser` during startup when host AppArmor is active. Those effects must be
admitted and reconciled before the candidate's inert gate can release; they cannot be omitted
because no build endpoint is called.

## Minimum dockerd branch under review

The following is a configuration **proposal**, not executable bytes. A later approval must freeze
the exact JSON, argv, environment, roots, permissions, and parser result and must reject every
unlisted/defaulted feature whose source can create a helper, connection, namespace, or background
mutation.

| Control | Required proposed value | Reason and limit |
|---|---|---|
| containerd address | one explicit private AF_UNIX address; embedded supervisor forbidden | avoids Moby's restart-capable containerd supervisor; does not prove a single transport |
| containerd namespaces | fixed `moby` task namespace; `plugins.moby` client remains unreachable | plugin configuration and APIs stay closed; any plugin-client dial rejects the target |
| listener and roots | one explicit private Engine AF_UNIX listener plus fresh fixed data, exec, PID, containerd root/state, and temporary paths | default/systemd listeners, inherited activation FDs, shared roots, and wrappers are excluded |
| `features.containerd-snapshotter` | `false` | keeps BuildKit and image operations off the second external snapshotter client |
| `features.containerd-migration` | `false` | prevents image-store migration and its additional state/client path |
| `storage-driver` | `vfs` | smallest classic graphdriver dependency surface; performance is irrelevant to one inert dummy container |
| `builder.gc.enabled` | `false` | empties the worker GC policy and prevents pruning; BuildKit still schedules its one-second no-policy callback and in-process drain/worker goroutines |
| CDI | disabled with an empty, protected specification source set | prevents device-spec scans and injections; parser/default closure remains a probe input |
| network | daemon bridge `none`; iptables, ip6tables, IP forwarding, IP masquerade, and userland proxy disabled | removes the default bridge/firewall/proxy path; the container additionally uses `NetworkMode=none` |
| cgroup and logging | `native.cgroupdriver=cgroupfs`; daemon and container log driver `none` | avoids a systemd/DBus and log-copier path; exact host compatibility and no-log behavior remain probe subjects |
| container profile | no host PID/IPC/network namespace, port, device, added capability, privilege, restart, live restore, health check, or init | retains one unprivileged inert PID 1; `init=false` still does not suppress the version probe |
| executables | exact minimal `PATH`, exact absolute `init-path`, exact shim/runc paths and interpreters/DSOs | `runc` is still looked up by its literal name in the stock-runtime startup path; path search is part of identity |
| telemetry | `OTEL_SDK_DISABLED=true`, `OTEL_TRACES_EXPORTER=none`, `OTEL_METRICS_EXPORTER=none`, no exporter endpoints | prevents BuildKit telemetry export; source/config and live no-egress proof remain conjunctive |
| host AppArmor | absent/disabled and proved before daemon launch | otherwise dockerd can execute `apparmor_parser`; accepting that helper would require a different reviewed population |
| reload/restart | disabled and hostile | configuration reload, daemon restart, live restore, reconnect, replacement, or recovered shim rejects the VM |

The source-audit draft uses these exact semantic keys; the paths are namespace examples, not an
approved target or file:

```json
{
  "hosts": ["unix:///run/dks/docker.sock"],
  "data-root": "/var/lib/dks/docker",
  "exec-root": "/run/dks/docker",
  "pidfile": "/run/dks/docker.pid",
  "containerd": "/run/dks/containerd/containerd.sock",
  "containerd-namespace": "moby",
  "containerd-plugins-namespace": "plugins.moby",
  "features": {
    "containerd-snapshotter": false,
    "containerd-migration": false,
    "cdi": false
  },
  "storage-driver": "vfs",
  "bridge": "none",
  "iptables": false,
  "ip6tables": false,
  "ip-forward": false,
  "ip-masq": false,
  "userland-proxy": false,
  "exec-opts": ["native.cgroupdriver=cgroupfs"],
  "default-runtime": "runc",
  "init": false,
  "init-path": "/opt/dks/bin/docker-init",
  "log-driver": "none",
  "live-restore": false,
  "debug": false,
  "builder": {"gc": {"enabled": false}}
}
```

Docker's daemon reference documents the external `--containerd` address and the network, storage,
runtime, init, and feature controls
([dockerd reference](https://docs.docker.com/reference/cli/dockerd/)). The tagged Moby sources remain
the authority for branch and side-effect reasoning
([image-store selection](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/image_store_choice.go),
[daemon construction](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/daemon.go),
[BuildKit controller](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/internal/builder-next/controller.go),
[Linux runtime setup](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/runtime_unix.go),
[info/version population](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/info_unix.go)).

## Containerd plugin and service hypothesis

The future private containerd configuration must start only the following source-required
population, plus the server's proved health registration. This is an initialization hypothesis, not
a ready TOML allowlist: containerd's dependency graph and configuration validation can still reject
it, and only a later dummy probe can establish the actual initialized set.

| Layer | Expected minimum |
|---|---|
| core plugins | content store, event exchange, Bolt metadata, GC scheduler, lease manager, Bolt mount manager, deprecation warnings, shim manager, runtime-v2 task manager |
| local services | containers, content, namespaces, tasks |
| gRPC services | containers, content, events, leases, namespaces, tasks, version; health only for dockerd's reconnect reachability check |
| runtime transport | one private gRPC listener and one distinct private ttrpc listener for shim publication |

The conservative exact-ID hypothesis is:

```text
io.containerd.content.v1.content
io.containerd.event.v1.exchange
io.containerd.metadata.v1.bolt
io.containerd.gc.v1.scheduler
io.containerd.lease.v1.manager
io.containerd.mount-manager.v1.bolt
io.containerd.warning.v1.deprecations
io.containerd.shim.v1.manager
io.containerd.runtime.v2.task
io.containerd.service.v1.containers-service
io.containerd.service.v1.content-service
io.containerd.service.v1.namespaces-service
io.containerd.service.v1.tasks-service
io.containerd.grpc.v1.containers
io.containerd.grpc.v1.content
io.containerd.grpc.v1.events
io.containerd.grpc.v1.leases
io.containerd.grpc.v1.namespaces
io.containerd.grpc.v1.tasks
io.containerd.grpc.v1.version
io.containerd.grpc.v1.healthcheck
```

The exact Bolt mount-manager plugin URI is `io.containerd.mount-manager.v1.bolt`. The runtime-v2
task manager obtains that service even though the classic graphdriver supplies rootfs mounts. The
lease manager depends on the GC scheduler. Plugin `Requires` declarations establish initialization
ordering, not proof that every plugin of the required type exists. In the classic branch, metadata
can initialize with no snapshotter and the task service consumes supplied rootfs mounts.

The expected excluded population is every snapshotter, image, snapshot, diff, transfer, CRI, NRI,
sandbox, restart, streaming, introspection, and network service or plugin, together with
the mounts gRPC facade. CRI and NRI must be explicitly disabled. The BuildKit local adapter is not a
containerd plugin. If the exact 2.2.6 initialization graph requires any excluded component, the
hypothesis is wrong and must be re-reviewed; no automatic broadening is allowed.

Namespaces cannot be excluded. Moby supplies a default `moby` namespace but no default runtime to
the main containerd client. Before applying Moby's explicit runtime option, containerd
`NewContainer` resolves its default runtime through the namespace label, which reaches the
namespaces service/RPC on the first create path. The expected cache and exact RPC count/order remain
a live probe obligation; the closed graph must admit and ledger at least that first label read.

`disabled_plugins` accepts exact IDs and no wildcard, while `required_plugins` checks requirements
rather than creating an allowlist. The approved future mechanism must therefore compare the exact
initialized inventory against the list above. Missing task-monitor support resolves to the no-op
monitor in this source branch, so the cgroups task-monitor plugin is not admitted. Containerd GC
remains a required asynchronous filesystem actor. Disabling Docker builder GC prevents worker
pruning but does not remove BuildKit's scheduled no-policy callback or its in-process goroutines.

Content and metadata live below the corresponding plugin roots; mount-manager state is
`<state>/io.containerd.mount-manager.v1.bolt/mounts.db`. The runtime-v2 shim socket is an important
exception to a naive private-state-root claim: its source-derived address is under
`/run/containerd/s/<digest-of-containerd-address,namespace,container-id>`. The exact address bytes,
mount namespace, permissions, holder graph, and absence of aliases remain live evidence.

Relevant tagged source authorities are containerd's
[plugin type constants](https://github.com/containerd/containerd/blob/v2.2.6/plugins/types.go),
[server assembly](https://github.com/containerd/containerd/blob/v2.2.6/cmd/containerd/server/server.go),
[metadata plugin](https://github.com/containerd/containerd/blob/v2.2.6/plugins/metadata/plugin.go),
[lease manager](https://github.com/containerd/containerd/blob/v2.2.6/plugins/leases/local.go),
[mount manager](https://github.com/containerd/containerd/blob/v2.2.6/plugins/mount/manager.go),
[namespace service](https://github.com/containerd/containerd/blob/v2.2.6/plugins/services/namespaces/local.go),
[task service](https://github.com/containerd/containerd/blob/v2.2.6/plugins/services/tasks/local.go),
[runtime-v2 task manager](https://github.com/containerd/containerd/blob/v2.2.6/core/runtime/v2/task_manager.go),
and [shim socket derivation](https://github.com/containerd/containerd/blob/v2.2.6/pkg/shim/util_unix.go).
The future review must bind exact file and commit identities rather than relying on this category
summary.

## Closed Engine request state machine

API negotiation, redirects, retries, upgrades, hijacks, pipelining, and transparent connection
replacement remain forbidden. `E1` owns one lifetime events stream, `E2` owns one wait, and `E3`
serializes every finite request. The normal schedule has exactly 22 requests; the single hostile
kill schedule has exactly 23.

| Step | Socket | Request or non-Engine cut |
|---:|---|---|
| 1 | `E1` | open `GET /v1.55/events` with exact `type=container` and unique probe-label filters |
| 2 | `E3` | `GET /v1.55/version` #1 |
| 3 | `E3` | `GET /v1.55/info` #1 |
| 4 | `E3` | inspect the independently preloaded immutable `sha256:<64-hex>` image ID |
| 5 | `E3` | label- and name-filtered all-container list; require absence |
| 6 | `E3` | label- and name-filtered volume list; require absence |
| 7 | `E3` | create the named probe volume |
| 8 | `E3` | inspect that volume |
| 9 | `E3` | create the named, labelled, inert probe container |
| 10 | `E3` | inspect; require the exact created state |
| 11 | `E3` | start |
| — | non-Engine | await the authenticated inert-gate armed report |
| 12 | `E3` | inspect; require the exact armed/running state |
| 13 | `E2` | issue wait-for-not-running and hold it outstanding |
| 14 | `E3` | `GET /v1.55/version` #2 |
| 15 | `E3` | `GET /v1.55/info` #2 |
| — | non-Engine | record and perform the normal inert-gate release |
| — | `E1,E2` | receive the exact labelled container event and wait response |
| 16 | `E3` | inspect; require the exact exited state |
| 17 | `E3` | delete the container with `force=0&v=0` |
| 18 | `E3` | label-filtered all-container list; require absence |
| 19 | `E3` | delete the volume with `force=0` |
| 20 | `E3` | label-filtered volume list; require absence |
| 21 | `E3` | `GET /v1.55/version` #3 |
| 22 | `E3` | `GET /v1.55/info` #3 |

The hostile unreleased case inserts exactly one `E3`
`POST /v1.55/containers/{id}/kill?signal=SIGKILL` immediately after step 15 instead of performing
the non-Engine release; the remaining observation and cleanup sequence is unchanged and shifts by
one. Thus `E1=1`, `E2=1`, and `E3=20` normally or `E3=21` for that hostile case. Normal wait status is
zero; hostile wait status is 137. `E1` closes only after request 22/23 and that close is not another
HTTP request. The post-start version/info cut is intentional: checking both only before start would
miss a runtime edge created by start.

Moby copies container labels into container events, and its event filter matches both event type and
labels. On a fresh dedicated VM, the conjunction `type=container` plus the exact unique attempt label
can therefore close the accepted `E1` event class without accepting background volume or network
events. The normal accepted matching action set is exactly `create,start,die,destroy`; the hostile
case additionally accepts `kill`. Inspect, list, and wait remain authoritative, while events are
corroborating evidence. Exact canonical filter JSON, URL encoding with uppercase percent hex, query
order, headers, create bodies, response caps, deadlines, state fields, and goldens remain successor-
protocol blockers
([event filtering](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/events/filter.go),
[event label population](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/events.go),
[Engine API v1.55](https://github.com/moby/moby/blob/docker-v29.6.2/api/swagger.yaml)).

The create body must bind a single static, signed PID 1 rather than a shell: exact immutable image
ID and entrypoint; attempt label; numeric unprivileged user; `CapDrop=["ALL"]`; no auto-remove,
init, health check, restart, device or published port; log type `none`; one named `nocopy` probe
volume at the gate path; `NetworkMode=none`; read-only rootfs; exact runc runtime; and
`no-new-privileges=true`. Its bytes and parser result remain unapproved until the executable/image
and host security policy close.

## Legitimate process and connection population

| Source cut | Required expected population | Current evidence limit |
|---|---|---|
| dockerd startup | two `runc features` executions, one for each stock runtime name | source-derived cardinality; exact argv/env/files and successful results unproved |
| BuildKit construction | eleven embedded payload attempts under protected `TMPDIR/qemu-check*`: native `amd64`, then `arm64,riscv64,ppc64,ppc64le,s390x,386,mips64le,mips64,loong64,arm` | exact fork/exec history, amd64 ISA result, native-compat outcome, and absence of a `binfmt_misc` interpreter require a target probe |
| six info/version requests | six `runc --version`, six exact-init `--version`, six containerd Version RPCs | source-derived; every child and RPC must join the API-step ledger |
| first container create | at least one containerd namespace-label Get RPC before the explicit runtime option is applied | source-derived route; cache behavior and exact RPC order remain probe-bound |
| task create/start | transient shim `start`, one persistent shim, transient runc `create` and `start`, runc init/re-exec stages becoming container PID 1, and one containerd-to-shim ttrpc client | exact counts, descriptors, mounts, and process order remain unproved |
| first shim event | one lazy reverse shim-to-containerd ttrpc publisher connection | retry/reconnect exists in source; first connection must precede release and every retry rejects |
| task delete | transient runc `delete`, terminal runtime/shim delete operations, and persistent-shim exit; hostile adds runc `kill` | forced delete or a shim delete helper on the nominal path is adverse; exact cleanup order remains unproved |
| AppArmor-active startup | possible `apparmor_parser` child despite per-container unconfined policy | narrow target requires AppArmor absent/disabled; otherwise the helper must be separately pinned and counted |

BuildKit's architecture checks are not harmless library calls: the source writes and executes the
native amd64 probe, then writes and attempts the ten other embedded probe binaries in a chroot. The
native probe's nonzero exit encodes amd64 ISA support and is expected evidence, not a failed exec. A
registered `binfmt_misc` handler can add a QEMU or other interpreter process and executable
dependency. The proposed target must therefore prove
`binfmt_misc` absent/empty before dockerd starts or explicitly bind and qualify the additional
interpreters; the current narrow direction chooses absence. Protect `TMPDIR` and reconcile every
probe child and expected exit. The `386` payload may execute via amd64 compatibility even with
`binfmt_misc` empty. Disabling build routes or builder GC does not remove these startup effects. The
exact dockerd artifact must also identify every embedded probe byte; the tagged source alone is
insufficient
([BuildKit platform detection](https://github.com/moby/moby/blob/docker-v29.6.2/vendor/github.com/moby/buildkit/util/archutil/detect.go),
[Unix probe execution](https://github.com/moby/moby/blob/docker-v29.6.2/vendor/github.com/moby/buildkit/util/archutil/check_unix.go)).

The persistent shim's event publisher and the main dockerd containerd client support reconnect.
Health/readiness checks that create another client are forbidden. Readiness uses non-connecting
process/socket evidence, while the existing main-client health RPC is only part of reconnection
reachability after an event-stream error; the accepted path must not exercise it. Source establishes
a logical graph, never a permanent one-socket claim
([Moby remote client](https://github.com/moby/moby/blob/docker-v29.6.2/daemon/internal/libcontainerd/remote/client.go),
[shim publisher](https://github.com/containerd/containerd/blob/v2.2.6/pkg/shim/publisher.go),
[runc-v2 manager](https://github.com/containerd/containerd/blob/v2.2.6/cmd/containerd-shim-runc-v2/manager/manager_linux.go)).

The schedule does not source-require exec, pause/resume, stats, checkpoint, idmapped-mount, CNI,
userland-proxy, firewall, or systemd/DBus helpers. Observation of one is adverse rather than a reason
to expand the accepted set during a run.

## Remaining blockers and disposition

The static audit closes enough source questions to reject the default snapshotter path and to name
one narrower classic-graphdriver hypothesis. It does **not** authorize configuration, download, or
execution. At least these blockers remain:

1. exact dockerd/containerd JSON/TOML, argv, environment, protected roots, parser result, disabled-
   plugin list, initialized-plugin transcript, and source/build/SBOM/executable/interpreter/DSO
   identities;
2. independently acquired OCI image/config/layer/rootfs identities, inert PID 1 bytes, mount plan,
   and proof that no pull, unpack, snapshotter, network, DNS, credential, or registry path runs;
3. exact physical gRPC/ttrpc/socketpair population, lazy-dial order, keepalive settings, AF_UNIX
   identities, shim grouping annotations, and fail-on-reconnect evidence;
4. exact helper/runc subcommands, argv/env/open/mount/descriptor/cgroup histories, BuildKit probe
   order, `binfmt_misc`/native-compat and AppArmor state, embedded probe identities, containerd GC,
   the scheduled BuildKit no-policy GC callback, and zero observer loss;
5. exact 22/23 raw HTTP requests and bounded responses, keepalive behavior on the three sealed
   sockets, deadlines, goldens, event action, and cleanup/absence oracle;
6. provider/kernel/BPF/gate/external-term identities and a separately approved, independently run,
   redb-free dummy mechanism probe with all hostile fixtures.

If the frozen tuple cannot initialize and finish the exact request schedule using only this
population and the three preconnected Engine sockets, `D20,D21` is infeasible. The protocol must not
enable another plugin, helper, socket, retry, or connection as an unreviewed fallback. Even a passing
dummy probe would establish only mechanism eligibility for later approval work. It would not select
redb, execute a backend candidate, validate state latency or crash recovery, change cluster
admission, provide exactly-once delivery, or replace the independent production soak.
