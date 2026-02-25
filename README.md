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
and cold data in S3-compatible object storage. It also supports **KV Store** and **Object Store** tiering with transparent client-side fallback.

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
| KV Store support | Key-based indexing, revision history, prefix scan for `KV_*` streams |
| Object Store support | Chunk reassembly, metadata indexing for `OBJ_*` streams |
| Transparent client | `pkg/nts` library wraps JetStream with automatic cold fallback |
| HTTP REST API | Query messages, KV keys, objects, list blocks, trigger demote/promote |
| NATS request-reply | `nts.get.*`, `nts.kv.*`, `nts.obj.*` subjects |
| Prometheus metrics | `nts_*` metrics for ingestion, tier usage, KV/Object ops, latency |
| Health checks | Liveness (`/healthz`) and readiness (`/readyz`) endpoints |
| Management CLI | `nts-ctl` for inspecting streams, blocks, KV keys, and objects |
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

### KV Store tiering

```bash
# Create a KV bucket and put some keys
nats kv add config --history=5
nats kv put config app.port "8080"
nats kv put config app.port "9090"
nats kv put config app.host "localhost"

# After the sidecar archives them, query via cold storage:
curl -s localhost:8080/v1/kv/config/get/app.port | jq .
curl -s localhost:8080/v1/kv/config/keys | jq .
curl -s localhost:8080/v1/kv/config/history/app.port | jq .

# Or via NATS
nats req nts.kv.config.get.app.port ""
nats req nts.kv.config.keys ""

# CLI
nts-ctl kv get config app.port
nts-ctl kv keys config
nts-ctl kv history config app.port
```

### Object Store tiering

```bash
# Create an Object Store bucket and put a file
nats object add files
nats object put files /path/to/report.pdf --name=report.pdf

# After archival, retrieve via cold storage:
curl -s localhost:8080/v1/objects/files/info/report.pdf | jq .
curl -so report.pdf localhost:8080/v1/objects/files/get/report.pdf
curl -s localhost:8080/v1/objects/files/list | jq .

# Or via NATS
nats req nts.obj.files.info.report.pdf ""
nats req nts.obj.files.list ""

# CLI
nts-ctl obj info files report.pdf
nts-ctl obj list files
nts-ctl obj get files report.pdf > report.pdf
```

### Transparent client library (`pkg/nts`)

For Go applications, the `pkg/nts` library wraps `jetstream.KeyValue` and `jetstream.ObjectStore` with automatic fallback to cold storage:

```go
import "github.com/gftdcojp/nats-tiered-storage/pkg/nts"

client, _ := nts.New(nts.Config{NC: nc, JS: js})

// KV — falls back to sidecar when key is not in JetStream
kv, _ := client.KeyValue(ctx, "config")
entry, _ := kv.Get(ctx, "app.port")  // returns from cold storage transparently
fmt.Println(string(entry.Value()))    // "9090"

// Object Store — reassembles chunks from cold storage
obs, _ := client.ObjectStore(ctx, "files")
reader, _ := obs.Get(ctx, "report.pdf")
io.Copy(dst, reader)

// Direct message retrieval
msg, _ := client.GetMessage(ctx, "ORDERS", 42)
```

## Configuration

Configuration is via YAML file. See [`configs/example.yaml`](configs/example.yaml) for a full reference.

### Minimal example

```yaml
nats:
  url: "nats://localhost:4222"

streams:
  # Regular stream (auto-detected)
  - name: "ORDERS"
    consumer_name: "nts-archiver"
    tiers:
      memory: { enabled: true, max_bytes: "256MB", max_age: "5m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data", max_age: "24h" }
      blob:   { enabled: true, bucket: "nats-tiered-archive", region: "us-east-1" }

  # KV Store (auto-detected from KV_ prefix)
  - name: "KV_config"
    consumer_name: "nts-archiver-kv"
    kv:
      index_all_revisions: true  # keep full history
    tiers:
      memory: { enabled: true, max_age: "10m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data" }

  # Object Store (auto-detected from OBJ_ prefix)
  - name: "OBJ_files"
    consumer_name: "nts-archiver-obj"
    tiers:
      file: { enabled: true, data_dir: "/var/lib/nts/data" }
      blob: { enabled: true, bucket: "nats-archive", region: "us-east-1" }

block:
  target_size: "8MB"
  max_linger: "30s"

metadata:
  path: "/var/lib/nts/meta.db"
```

Stream types are auto-detected from the name prefix (`KV_` → kv, `OBJ_` → objectstore), or can be set explicitly with the `type` field.

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

### Stream / Block APIs

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

### KV Store APIs

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/kv/{bucket}/get/{key}` | Get latest value for a key |
| `GET` | `/v1/kv/{bucket}/keys?prefix=...` | List keys (optionally filtered by prefix) |
| `GET` | `/v1/kv/{bucket}/history/{key}` | Get all revisions of a key |

### Object Store APIs

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/objects/{bucket}/get/{name}` | Download object (binary streaming) |
| `GET` | `/v1/objects/{bucket}/info/{name}` | Get object metadata |
| `GET` | `/v1/objects/{bucket}/list` | List all objects |

### NATS Request-Reply Subjects

| Subject | Description |
|---|---|
| `nts.get.{stream}.{seq}` | Retrieve message by sequence |
| `nts.kv.{bucket}.get.{key}` | Get KV value |
| `nts.kv.{bucket}.keys` | List KV keys |
| `nts.kv.{bucket}.history.{key}` | Get KV key history |
| `nts.obj.{bucket}.get.{name}` | Get object (reassembled) |
| `nts.obj.{bucket}.info.{name}` | Get object metadata |
| `nts.obj.{bucket}.list` | List objects |

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

## Using with an Existing JetStream Environment

`nats-tiered-storage` is non-invasive — it connects as a standard NATS client and requires **no changes** to your existing `nats-server` or streams.

### Step 1: Identify your streams

```bash
# List existing JetStream streams
nats stream ls

# List KV buckets (internal stream name: KV_{bucket})
nats kv ls

# List Object Store buckets (internal stream name: OBJ_{bucket})
nats object ls
```

### Step 2: Create a configuration file

Point the sidecar at your existing NATS server and list the streams you want to archive:

```yaml
nats:
  url: "nats://your-nats-server:4222"
  # For authenticated environments:
  # credentials_file: "/path/to/user.creds"
  # nkey_seed_file: "/path/to/seed"
  # tls:
  #   ca_file: "/path/to/ca.pem"
  #   cert_file: "/path/to/cert.pem"
  #   key_file: "/path/to/key.pem"

streams:
  - name: "ORDERS"                      # your existing stream name
    consumer_name: "nts-archiver"        # new durable consumer (auto-created)
    tiers:
      memory: { enabled: true, max_bytes: "256MB", max_age: "5m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data", max_age: "24h" }
      blob:   { enabled: true, bucket: "my-archive", region: "us-east-1" }

  - name: "KV_config"                   # existing KV bucket → stream name
    consumer_name: "nts-archiver-kv"
    kv:
      index_all_revisions: true
    tiers:
      memory: { enabled: true, max_age: "10m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data" }

  - name: "OBJ_files"                   # existing Object Store → stream name
    consumer_name: "nts-archiver-obj"
    tiers:
      file: { enabled: true, data_dir: "/var/lib/nts/data" }
      blob: { enabled: true, bucket: "my-archive", region: "us-east-1" }

block:
  target_size: "8MB"
  max_linger: "30s"

metadata:
  path: "/var/lib/nts/meta.db"

api:
  enabled: true
  listen: ":8080"
  nats_responder:
    enabled: true
    subject_prefix: "nts"
```

### Step 3: Deploy the sidecar

The sidecar can run anywhere that has network access to your NATS server:

```bash
# Option A: Binary
make build
bin/nats-tiered-storage -config /path/to/config.yaml

# Option B: Docker
docker run -v /path/to/config.yaml:/etc/nts/config.yaml \
           -v /var/lib/nts:/var/lib/nts \
           nats-tiered-storage

# Option C: Kubernetes sidecar (same pod as nats-server)
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/sidecar-deployment.yaml
```

### Step 4: Access cold data

Once the sidecar is running, it automatically creates durable Pull Consumers on your streams and begins archiving. You can access archived data in three ways:

**HTTP API** — no application changes needed:
```bash
curl -s localhost:8080/v1/messages/ORDERS/12345 | jq .
curl -s localhost:8080/v1/kv/config/get/app.port | jq .
curl -so report.pdf localhost:8080/v1/objects/files/get/report.pdf
```

**NATS request-reply** — works with any NATS client in any language:
```bash
nats req nts.get.ORDERS.12345 ""
nats req nts.kv.config.get.app.port ""
nats req nts.obj.files.get.report.pdf ""
```

**Go client library** — transparent fallback with minimal code changes:
```go
import "github.com/gftdcojp/nats-tiered-storage/pkg/nts"

// Wrap your existing NATS connection
client, _ := nts.New(nts.Config{NC: nc, JS: js})

// Replace js.KeyValue() → client.KeyValue()
// When a key is no longer in JetStream, it falls back to cold storage automatically
kv, _ := client.KeyValue(ctx, "config")
entry, _ := kv.Get(ctx, "app.port")

// Replace js.ObjectStore() → client.ObjectStore()
obs, _ := client.ObjectStore(ctx, "files")
reader, _ := obs.Get(ctx, "report.pdf")
```

### How it works with existing streams

```
┌─────────────────────────────────────────────────────────┐
│ Existing Environment (no changes required)              │
│                                                         │
│  Your App ──publish/subscribe──> nats-server (JetStream) │
│                                      │                  │
│                                      │ Pull Consumer    │
│                                      ▼                  │
│                              nats-tiered-storage        │
│                              ┌──────────────────┐       │
│                              │ Memory (hot)     │       │
│                              │ File   (warm)    │       │
│                              │ S3     (cold)    │       │
│                              └──────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

Key points:

- The sidecar creates its own **durable Pull Consumer** — it does not interfere with your existing consumers or subscriptions
- Messages remain in JetStream after the sidecar ACKs them — the sidecar's consumer is independent
- You can safely set `max_age` or `max_bytes` on JetStream streams to limit retention; the sidecar archives messages before they expire
- Stream type is auto-detected from the name prefix (`KV_` → kv, `OBJ_` → objectstore)

### Typical use case: extend retention beyond JetStream limits

```
JetStream:  max_age=7d  (messages deleted after 7 days)
Sidecar:    archives on ingest → Memory 5m → File 24h → S3 forever

Result: Hot data served from JetStream (fast),
        cold data served from sidecar (transparent fallback)
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
| `nts_tier_block_count` | Gauge | Current block count by tier |
| `nts_tier_bytes` | Gauge | Current bytes by tier |
| `nts_demotion_ops_total` | Counter | Tier demotions |
| `nts_promotion_ops_total` | Counter | Tier promotions |
| `nts_read_requests_total` | Counter | Read requests by tier |
| `nts_read_latency_seconds` | Histogram | Read latency by tier |
| `nts_s3_upload_duration_seconds` | Histogram | S3 upload latency |
| `nts_kv_index_ops_total` | Counter | KV index operations |
| `nts_kv_get_requests_total` | Counter | KV sidecar get requests |
| `nts_obj_index_ops_total` | Counter | Object Store index operations |
| `nts_obj_get_requests_total` | Counter | Object Store sidecar get requests |
| `nts_obj_reassembly_duration_seconds` | Histogram | Object chunk reassembly time |

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
│   ├── config/                 # YAML config parsing, stream type detection
│   ├── file/                   # Local file tier store
│   ├── ingest/                 # JetStream consumer, pipeline, KV/Obj classifiers
│   ├── lifecycle/              # Retention enforcement and GC
│   ├── memory/                 # In-process LRU memory tier store
│   ├── meta/                   # BoltDB metadata (blocks, KV index, Obj index)
│   ├── metrics/                # Prometheus metrics and health checks
│   ├── serve/                  # HTTP + NATS responders (stream, KV, Object Store)
│   ├── tier/                   # Tier controller and policy engine
│   └── types/                  # Shared types (Tier, BlockRef, StoredMessage)
├── pkg/
│   ├── natsutil/               # NATS connection helper
│   ├── nts/                    # Transparent client library (KV/Obj fallback)
│   └── s3util/                 # S3 client factory
├── configs/                    # Example configuration files
└── deploy/
    ├── docker/                 # Dockerfile and docker-compose
    └── kubernetes/             # K8s manifests
```

## Testing

The project has **149 tests** covering all packages with race detection enabled.

```bash
# All tests (unit + integration + durability)
go test -race -count=1 ./...

# Unit tests only
make test

# Integration tests (embedded nats-server, no Docker required)
go test -race -count=1 -run TestIntegration ./internal/

# Durability tests (restart survival, CRC, tier transitions, KV/Obj persistence)
go test -race -count=1 -run TestDurability ./internal/

# Stress tests (high volume, concurrency, rapid tier migration, large blocks)
go test -race -count=1 -tags stress -run TestStress ./internal/

# Coverage report
go test -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# Development stack with Docker Compose
make dev
```

### Test categories

| Category | Tests | Description |
|---|---|---|
| Block format | 9 | Encode/decode, builder, index, CRC checksums |
| Memory store | 8 | LRU eviction, MaxBlocks, concurrent access |
| File store | 15 | Put/Get, index lookup, corruption fallback, durability |
| Blob store | 12 | S3 mock, range requests, cache, concurrent race detection |
| Tier controller | 17 | Ingest, demote, promote, retrieve, policy cycles, concurrency |
| Lifecycle/GC | 6 | Expiry, retention, partial failure, cancel |
| HTTP handlers | 12 | Status, blocks, messages, KV, Object Store, errors |
| Health/Metrics | 7 | Liveness, readiness, NATS/meta checks, Prometheus metrics |
| Client (`pkg/nts`) | 16 | Error helpers, KV/Obj sidecar fallback, embedded NATS |
| Integration | 3 | Full pipeline, KV Store, Object Store (end-to-end) |
| Durability | 7 | BoltDB restart, file restart, CRC, tier transitions, KV/Obj persistence |
| Stress | 6 | 10K msg ingest, 50-goroutine concurrency, rapid tier migration, 50K msg blocks |
| Config | 4 | YAML parsing, validation, byte sizes |

## License

Apache License 2.0 — see [LICENSE](LICENSE).
