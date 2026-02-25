# nats-tiered-storage

Tiered storage sidecar for [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).
Automatically archives JetStream messages through **Memory → File → Blob (S3-compatible)** tiers based on configurable policies.

## Background

NATS JetStream does not natively support tiered storage — a feature long requested by the community
([#3871](https://github.com/nats-io/nats-server/discussions/3871),
[#6478](https://github.com/nats-io/nats-server/discussions/6478),
[#3772](https://github.com/nats-io/nats-server/discussions/3772)).

`nats-tiered-storage` runs as a sidecar alongside `nats-server` and transparently
moves data between tiers — keeping hot data in memory, warm data on local disk,
and cold data in S3-compatible object storage.

## Architecture

```
                                +---------------------------+
                                |   nats-tiered-storage     |
                                |      (Go sidecar)         |
                                |                           |
Clients ──> nats-server <────>  |  ┌─────────────────────┐  |     ┌──────────────────┐
     (publish/subscribe)        |  │   Tier Controller    │  |     │  S3-Compatible    │
                                |  ├────────┬──────┬──────┤  |────>│  Object Storage   │
                                |  │ Memory │ File │ Blob │  |     │  (AWS/MinIO/R2)   │
                                |  └────────┴──────┴──────┘  |     └──────────────────┘
                                |  ┌─────────────────────┐  |
                                |  │  Metadata (BoltDB)  │  |
                                |  └─────────────────────┘  |
                                +---------------------------+
```

### Design Principles

- **Non-invasive** — connects as a standard NATS client; no server plugins or filesystem hooks
- **Consumer-based ingest** — uses JetStream durable Pull Consumers to fetch messages
- **Block-aligned writes** — groups messages into ~8 MB blocks before uploading to S3 (minimizes per-request cost)
- **ACK-after-ingest** — acknowledges messages only after the block is durably stored (no data loss)

## Features

| Feature | Description |
|---|---|
| 3-tier hierarchy | Memory (LRU) → File (local disk) → Blob (S3/MinIO/R2) |
| Policy-based demotion | Age, size, and block-count thresholds per tier |
| On-demand promotion | Cold reads automatically warm data back into hotter tiers |
| HTTP REST API | Query messages, list blocks, trigger demote/promote |
| NATS request-reply | `nats req nts.get.{stream}.{seq} ""` |
| Prometheus metrics | `nts_*` metrics for ingestion, tier usage, latency |
| Health checks | Liveness (`/healthz`) and readiness (`/readyz`) endpoints |
| Management CLI | `nts-ctl` for inspecting streams, blocks, and tier state |
| Multi-stream | Configure independent policies per JetStream stream |
| Graceful shutdown | Flushes partial blocks on SIGINT/SIGTERM |

## Quick Start

### Prerequisites

- Go 1.22+
- Docker & Docker Compose (for development stack)

### Build from source

```bash
make build
# Outputs: bin/nats-tiered-storage, bin/nts-ctl
```

### Run with Docker Compose

The included `docker-compose.yaml` starts a full development stack: nats-server, MinIO, and nats-tiered-storage.

```bash
make dev
```

This brings up:
- **NATS** — `localhost:4222` (client), `localhost:8222` (monitoring)
- **MinIO** — `localhost:9000` (API), `localhost:9001` (console, user: `minioadmin`/`minioadmin`)
- **nats-tiered-storage** — `localhost:8080` (HTTP API), `localhost:9090` (metrics), `localhost:8081` (health)

### Try it out

```bash
# Create a stream and publish some messages
nats stream add ORDERS --subjects "orders.>" --defaults
for i in $(seq 1 100); do
  nats pub orders.new "{\"id\": $i}"
done

# Check tier status via HTTP API
curl -s localhost:8080/v1/streams | jq .
curl -s localhost:8080/v1/blocks/ORDERS | jq .

# Retrieve a specific message
curl -s localhost:8080/v1/messages/ORDERS/1 | jq .

# Or via NATS request-reply
nats req nts.get.ORDERS.1 ""

# Use the CLI tool
nts-ctl -addr http://localhost:8080 status
nts-ctl -addr http://localhost:8080 streams
nts-ctl -addr http://localhost:8080 blocks ORDERS
```

## Configuration

Configuration is via YAML file. See [`configs/example.yaml`](configs/example.yaml) for a full reference.

### Minimal example

```yaml
nats:
  url: "nats://localhost:4222"

streams:
  - name: "ORDERS"
    consumer_name: "nts-archiver"
    tiers:
      memory:
        enabled: true
        max_bytes: "256MB"
        max_age: "5m"
      file:
        enabled: true
        data_dir: "/var/lib/nts/data"
        max_bytes: "10GB"
        max_age: "24h"
      blob:
        enabled: true
        bucket: "nats-tiered-archive"
        region: "us-east-1"

block:
  target_size: "8MB"
  max_linger: "30s"

metadata:
  path: "/var/lib/nts/meta.db"
```

### Key settings

| Setting | Default | Description |
|---|---|---|
| `block.target_size` | `8MB` | Target block size before sealing |
| `block.max_linger` | `30s` | Max time to wait before sealing a partial block |
| `block.compression` | `s2` | Compression algorithm (`none`, `s2`) |
| `policy.eval_interval` | `30s` | How often to evaluate demotion policies |
| `tiers.memory.max_age` | — | Demote memory blocks older than this |
| `tiers.memory.max_bytes` | — | Demote oldest memory blocks when total exceeds this |
| `tiers.file.max_age` | — | Demote file blocks older than this |
| `tiers.blob.storage_class` | `STANDARD` | S3 storage class (`STANDARD`, `STANDARD_IA`, `GLACIER`) |

## HTTP API

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/status` | Service status |
| `GET` | `/v1/streams` | List configured streams with tier counts |
| `GET` | `/v1/streams/{stream}/stats` | Per-tier byte/block stats |
| `GET` | `/v1/messages/{stream}/{seq}` | Retrieve a single message by sequence |
| `GET` | `/v1/messages/{stream}?start=N&count=M` | Retrieve a range of messages |
| `GET` | `/v1/blocks/{stream}` | List all blocks for a stream |
| `GET` | `/v1/blocks/{stream}/{blockID}` | Get block metadata |
| `POST` | `/v1/admin/demote/{stream}/{blockID}` | Demote a block to the next colder tier |
| `POST` | `/v1/admin/promote/{stream}/{blockID}` | Promote a block to the next hotter tier |

## Data Flow

### Ingest (write path)

```
nats-server JetStream
  → Pull Consumer (fetch batch)
    → Block Builder (accumulate to ~8MB or linger timeout)
      → Seal Block → Tier Controller.Ingest() → Memory Tier
        → Metadata Store.RecordBlock()
          → ACK batch
```

### Demotion (automatic)

```
Memory [age > 5m / max_bytes exceeded] → File Tier
File   [age > 24h / max_bytes exceeded] → Blob Tier (S3)
```

Order: write to destination → update metadata → delete from source (crash-safe).

### Retrieval (read path)

```
Request → Metadata Lookup → Tier routing
  → Memory hit  → return immediately
  → File hit    → return + async promote to memory
  → Blob hit    → S3 Range Request → return + async promote to file
```

## Deployment

### Docker

```bash
docker build -t nats-tiered-storage -f deploy/docker/Dockerfile .
docker run -v /path/to/config.yaml:/etc/nts/config.yaml nats-tiered-storage
```

### Kubernetes

Example manifests are provided in [`deploy/kubernetes/`](deploy/kubernetes/).

The sidecar is designed to run alongside `nats-server` in the same pod or as a separate Deployment:

```bash
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/sidecar-deployment.yaml
```

## Observability

### Prometheus Metrics

Key metrics (prefix `nts_`):

| Metric | Type | Description |
|---|---|---|
| `nts_messages_ingested_total` | Counter | Total messages ingested |
| `nts_blocks_sealed_total` | Counter | Total blocks sealed |
| `nts_blocks_current` | Gauge | Current block count by tier |
| `nts_bytes_current` | Gauge | Current bytes by tier |
| `nts_demotions_total` | Counter | Tier demotions |
| `nts_promotions_total` | Counter | Tier promotions |
| `nts_s3_upload_duration_seconds` | Histogram | S3 upload latency |
| `nts_consumer_lag` | Gauge | JetStream consumer lag |

### Health Checks

- **Liveness**: `GET :8081/healthz` — always returns 200 if the process is running
- **Readiness**: `GET :8081/readyz` — checks NATS connectivity, metadata store, and S3 (if configured)

## Project Structure

```
├── cmd/
│   ├── nats-tiered-storage/    # Main sidecar binary
│   └── nts-ctl/                # Management CLI
├── internal/
│   ├── block/                  # Block format encoder/decoder, builder, index
│   ├── blob/                   # S3-compatible blob tier store
│   ├── config/                 # YAML config parsing and validation
│   ├── file/                   # Local file tier store
│   ├── ingest/                 # JetStream consumer and ingest pipeline
│   ├── lifecycle/              # Retention enforcement and GC
│   ├── memory/                 # In-process LRU memory tier store
│   ├── meta/                   # BoltDB metadata store
│   ├── metrics/                # Prometheus metrics and health checks
│   ├── serve/                  # HTTP API and NATS request-reply responder
│   ├── tier/                   # Tier controller and policy engine
│   └── types/                  # Shared types (Tier, BlockRef, StoredMessage)
├── pkg/
│   ├── natsutil/               # NATS connection helper
│   └── s3util/                 # S3 client factory
├── configs/                    # Example configuration files
└── deploy/
    ├── docker/                 # Dockerfile and docker-compose
    └── kubernetes/             # K8s manifests
```

## Testing

```bash
# Unit tests
make test

# Integration test (embedded nats-server, no Docker required)
go test -race -count=1 -run TestIntegration ./internal/

# Development stack with Docker Compose
make dev
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
