# LaminarDB Helm Chart

A Helm chart for deploying [LaminarDB](https://laminardb.dev) (Embedded Streaming SQL Database) on Kubernetes.

## 🚀 Quick Start (Standalone / Standalone-Durable)

By default, the chart runs in `single` (standalone) mode.

```bash
helm repo add laminardb https://laminardb.io/charts
helm repo update
helm install my-laminardb laminardb/laminardb
```

For air-gapped / on-prem clusters, install from a local clone or a packaged tarball:
```bash
helm install my-laminardb deploy/helm/laminardb
# or: helm package deploy/helm/laminardb && helm install my-laminardb laminardb-*.tgz
```

---

## 🌐 Durable Clustered Setup (`cluster` mode)

LaminarDB supports distributed streaming execution across multiple nodes.

Cluster mode is still pre-production while the open security and operability work is completed. To run a durable 3-node cluster configuration for evaluation:

1. **Set `laminardb.mode` to `"cluster"`**
2. **Increase `replicaCount` to `3` (or more)**
3. **Configure a cluster-shared checkpoint URL**

Here is an example cluster values file (`cluster-values.yaml`):

```yaml
replicaCount: 3

laminardb:
  mode: cluster
  delivery: at_least_once
  logLevel: info
  keyGroups: 256
  
  checkpoint:
    interval: "30s"
    timeout: "120s"
    # Object store so checkpoints are recoverable across nodes (credentials via extraEnv).
    url: "az://laminardb-checkpoints/cluster"
    
  cluster:
    discovery:
      strategy: gossip
      gossipPort: 7946
      # seeds are generated from replicaCount (per-pod headless DNS names);
      # set `seeds` explicitly only for non-standard topologies

persistence:
  # The cluster uses the object store above, so it needs no checkpoint PVC.
  checkpoints:
    enabled: false

# Object-store credentials (example: Azure).
extraEnv:
  - name: AZURE_STORAGE_ACCOUNT_NAME
    valueFrom:
      secretKeyRef: { name: laminardb-azure, key: account }
  - name: AZURE_STORAGE_ACCOUNT_KEY
    valueFrom:
      secretKeyRef: { name: laminardb-azure, key: key }

guaranteedQoS: true

resources:
  limits:
    cpu: "4"
    memory: 8Gi
  requests:
    cpu: "4"
    memory: 8Gi

topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: laminardb
```

Apply this via:
```bash
helm install my-laminardb deploy/helm/laminardb -f cluster-values.yaml
```

---

## 🔑 Custom Secrets, Credentials, & Kafka Configuration

If you connect to external sources (like Kafka, Postgres CDC, or Azure Event Hubs), you can inject credential configurations via standard environment variables or files:

### 1. Environment Variable injection

```yaml
extraEnv:
  - name: KAFKA_BOOTSTRAP_SERVERS
    value: "kafka.default.svc.cluster.local:9092"
```

### 2. Loading from existing ConfigMaps / Secrets (`extraEnvFrom`)

For secret tokens, database credentials, or Azure SAS tokens, reference Kubernetes secrets directly:

```yaml
extraEnvFrom:
  - secretRef:
      name: laminardb-credentials
  - configMapRef:
      name: laminardb-global-config
```

---

## 📊 Monitoring & Alerts

The chart integrates natively with the Prometheus Operator:

```yaml
serviceMonitor:
  enabled: true
  interval: 15s

prometheusRule:
  enabled: true
  rules:
    - alert: LaminarDBClusterDown
      expr: up{job="laminardb"} == 0
      for: 2m
      labels:
        severity: critical
      annotations:
        summary: "LaminarDB instance is down on {{ $labels.pod }}"
```

---

## ⚙️ Configuration Reference

| Option | Description | Default |
|--------|-------------|---------|
| `replicaCount` | Number of pods to run | `1` |
| `podManagementPolicy` | StatefulSet pod launch ordering: `Parallel` or `OrderedReady` (immutable after creation) | `Parallel` |
| `laminardb.mode` | Server mode: `single` or `cluster` | `single` |
| `laminardb.logLevel` | Log level: `trace`, `debug`, `info`, `warn`, `error` | `info` |
| `laminardb.httpBind` | HTTP API bind address | `0.0.0.0:8080` |
| `laminardb.keyGroups` | Stable hash partitions; one node owns all in single mode and clusters distribute them | `256` |
| `laminardb.consoleToken.existingSecret` | Secret holding the console API bearer token (key from `secretKey`, default `token`); empty = unauthenticated | `""` |
| `laminardb.consoleCorsAllowedOrigins` | CORS allow-list of console origins; empty = permissive legacy policy | `[]` |
| `laminardb.delivery` | Pipeline-wide delivery: `best_effort`, `at_least_once`, or capability-gated `exactly_once` | `best_effort` |
| `laminardb.checkpoint.interval` | Checkpoint frequency | `30s` |
| `laminardb.checkpoint.url` | Provider-neutral checkpoint URL: `s3://` (including R2/MinIO), `gs://`, `az://`, `abfs(s)://`, or local `file://`. Empty = local default. | `""` |
| `laminardb.configWatch` | Hot-reload config on file change. Off in K8s (config changes roll pods via the checksum annotation); sets `LAMINAR_DISABLE_FILE_WATCH=1`. | `false` |
| `laminardb.cluster.discovery.strategy` | Discovery method (`gossip`, `static`) | `gossip` |
| `persistence.checkpoints.enabled` | Provision a dedicated checkpoints PVC. Off by default — prefer an object store via `laminardb.checkpoint.url`. | `false` |
| `guaranteedQoS` | Pin requests == limits for guaranteed CPU/Mem | `false` |
| `extraEnvFrom` | Inject variables from ConfigMaps / Secrets | `[]` |
| `extraVolumes` | Additional volumes to mount into pods | `[]` |

For `at_least_once` or `exactly_once`, a local `file://` checkpoint URL requires
`persistence.checkpoints.enabled=true`; the chart rejects an ephemeral combination at render time.
