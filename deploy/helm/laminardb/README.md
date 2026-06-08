# LaminarDB Helm Chart

A Helm chart for deploying [LaminarDB](https://laminardb.dev) (Embedded Streaming SQL Database) on Kubernetes.

## 🚀 Quick Start (Standalone / Standalone-Durable)

By default, the chart runs in `embedded` (single-node) mode.

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

## 🌐 Production Clustered Setup (`cluster` mode)

LaminarDB supports distributed execution where query processing and subscription routing are coordinated across multiple nodes.

To run a production-ready, durable 3-node cluster:

1. **Set `laminardb.mode` to `"cluster"`**
2. **Increase `replicaCount` to `3` (or more)**
3. **Configure durable state and checkpoints**

Here is an example production values file (`prod-values.yaml`):

```yaml
replicaCount: 3

laminardb:
  mode: cluster
  logLevel: info
  workers: 4
  
  state:
    backend: local
    path: "/var/lib/laminardb/state"
    
  checkpoint:
    interval: "30s"
    mode: aligned
    snapshotStrategy: incremental
    url: "file:///var/lib/laminardb/checkpoints"
    
  cluster:
    discovery:
      strategy: dns    # Headless service DNS discovery (recommended for K8s)
      gossipPort: 7946
    coordination:
      strategy: raft
      raftPort: 7947
      electionTimeout: "1500ms"
      heartbeatInterval: "300ms"

persistence:
  state:
    enabled: true
    storageClass: "managed-csi" # Azure Disk CSI / local SSD on-prem
    size: 50Gi
  checkpoints:
    enabled: true
    storageClass: "azurefile-csi" # Shared Azure Files CSI / CephFS on-prem
    size: 100Gi

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
helm install my-laminardb deploy/helm/laminardb -f prod-values.yaml
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
| `laminardb.mode` | Server mode: `embedded` or `cluster` | `embedded` |
| `laminardb.logLevel` | Log level: `trace`, `debug`, `info`, `warn`, `error` | `info` |
| `laminardb.httpBind` | HTTP API bind address | `0.0.0.0:8080` |
| `laminardb.workers` | Number of worker threads (0 = auto) | `0` |
| `laminardb.state.backend` | Storage type: `in_process`, `local`, or `object_store` | `local` |
| `laminardb.state.path` | Path for persistent state (required if backend=local) | `/var/lib/laminardb/state` |
| `laminardb.state.url` | URL for object storage (required if backend=object_store) | `""` |
| `laminardb.checkpoint.enabled` | Enable checkpoint coordination | `true` |
| `laminardb.checkpoint.interval` | Checkpoint frequency | `30s` |
| `laminardb.checkpoint.url` | Storage endpoint (`file://` or `s3://`) | `file:///var/lib/laminardb/checkpoints` |
| `laminardb.cluster.discovery.strategy` | Discovery method (`dns`, `gossip`, `static`) | `dns` |
| `laminardb.cluster.coordination.strategy` | Clustering controller coordination (`raft`) | `raft` |
| `persistence.state.enabled` | Keep local state in Persistent Volume | `true` |
| `persistence.state.storageClass` | K8s storage class for state PVC | `""` (default) |
| `persistence.checkpoints.enabled` | Keep checkpoints in Persistent Volume | `true` |
| `guaranteedQoS` | Pin requests == limits for guaranteed CPU/Mem | `false` |
| `extraEnvFrom` | Inject variables from ConfigMaps / Secrets | `[]` |
| `extraVolumes` | Additional volumes to mount into pods | `[]` |
