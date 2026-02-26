# nats-tiered-storage

[![Go Reference](https://pkg.go.dev/badge/github.com/gftdcojp/nats-tiered-storage/pkg/nts.svg)](https://pkg.go.dev/github.com/gftdcojp/nats-tiered-storage/pkg/nts)
[![Go Report Card](https://goreportcard.com/badge/github.com/gftdcojp/nats-tiered-storage)](https://goreportcard.com/report/github.com/gftdcojp/nats-tiered-storage)
[![GHCR](https://img.shields.io/badge/GHCR-v0.3.0-blue)](https://github.com/gftdcojp/nats-tiered-storage/pkgs/container/nats-tiered-storage)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Tiered storage sidecar for [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).
Automatically archives JetStream messages through **Memory → File → Blob (S3-compatible)** tiers using write-through fan-out and configurable eviction policies.

## Why

NATS JetStream does not natively support tiered storage — a feature long requested by the community
([#3871](https://github.com/nats-io/nats-server/discussions/3871),
[#6478](https://github.com/nats-io/nats-server/discussions/6478),
[#3772](https://github.com/nats-io/nats-server/discussions/3772)).

`nats-tiered-storage` runs as a sidecar alongside `nats-server` and transparently
manages data across tiers — writing to all enabled tiers on ingest (write-through)
and evicting from hotter tiers based on policy. Hot data is served from memory, warm data from local disk,
and cold data from S3-compatible object storage. It supports **Streams**, **KV Store**, and **Object Store** with transparent client-side fallback.

## Install

### Go client library

```bash
go get github.com/gftdcojp/nats-tiered-storage/pkg/nts@latest
```

```go
import "github.com/gftdcojp/nats-tiered-storage/pkg/nts"

client, _ := nts.New(nts.Config{NC: nc, JS: js})

// KV — falls back to sidecar when key is not in JetStream
kv, _ := client.KeyValue(ctx, "config")
entry, _ := kv.Get(ctx, "app.port")
fmt.Println(string(entry.Value()))

// Object Store — reassembles chunks from cold storage
obs, _ := client.ObjectStore(ctx, "files")
reader, _ := obs.Get(ctx, "report.pdf")
io.Copy(dst, reader)

// Direct message retrieval
msg, _ := client.GetMessage(ctx, "ORDERS", 42)
```

### Sidecar binary

```bash
make build
# bin/nats-tiered-storage   — sidecar daemon
# bin/nts-ctl               — management CLI
```

### Docker (GHCR)

```bash
docker pull ghcr.io/gftdcojp/nats-tiered-storage:0.3.0
docker run -v /path/to/config.yaml:/etc/nts/config.yaml \
  ghcr.io/gftdcojp/nats-tiered-storage:0.3.0
```

### Docker (local build)

```bash
docker build -t nats-tiered-storage -f deploy/docker/Dockerfile .
docker run -v /path/to/config.yaml:/etc/nts/config.yaml nats-tiered-storage
```

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
- **Write-through fan-out** — each ingested block is written to all enabled tiers simultaneously; demotion simply evicts from the hottest tier
- **Consumer-based ingest** — uses JetStream durable Pull Consumers to fetch messages; automatically mirrors WorkQueue streams to avoid consumer conflicts
- **Block-aligned writes** — groups messages into ~8 MB blocks before uploading to S3 (minimizes per-request cost)
- **ACK-after-ingest** — acknowledges messages only after the block is durably stored in all tiers (no data loss)

## Features

| Feature | Description |
|---|---|
| 3-tier hierarchy | Memory (LRU) → File (local disk) → Blob (S3/MinIO/R2) |
| Write-through ingest | Blocks written to all enabled tiers simultaneously on ingest |
| Policy-based eviction | Age, size, and block-count thresholds trigger eviction from hotter tiers |
| Fallthrough reads | Reads try hottest tier first, automatically fall through on miss |
| On-demand promotion | Explicitly promote cold blocks back into hotter tiers |
| KV Store support | Key-based indexing, revision history, prefix scan for `KV_*` streams |
| Object Store support | Chunk reassembly, metadata indexing for `OBJ_*` streams |
| Transparent client | [`pkg/nts`](https://pkg.go.dev/github.com/gftdcojp/nats-tiered-storage/pkg/nts) library wraps JetStream with automatic cold fallback |
| HTTP REST API | Query messages, KV keys, objects, list blocks, trigger demote/promote |
| NATS request-reply | `nts.get.*`, `nts.kv.*`, `nts.obj.*` subjects |
| Prometheus metrics | `nts_*` metrics for ingestion, tier usage, KV/Object ops, latency |
| Health checks | Liveness (`/healthz`) and readiness (`/readyz`) endpoints |
| Management CLI | `nts-ctl` for inspecting streams, blocks, KV keys, and objects |
| WorkQueue auto-mirror | Automatically creates a Limits-retention mirror for WorkQueue streams with existing consumers |
| Multi-stream | Configure independent policies per JetStream stream |
| Graceful shutdown | Flushes partial blocks on SIGINT/SIGTERM |

## Quick Start

### Prerequisites

- Go 1.22+
- Docker & Docker Compose (for development stack)

### Run with Docker Compose

```bash
make dev
```

This brings up:
- **NATS** — `localhost:4222` (client), `localhost:8222` (monitoring)
- **MinIO** — `localhost:9000` (API), `localhost:9001` (console, user: `minioadmin`/`minioadmin`)
- **nats-tiered-storage** — `localhost:8080` (HTTP API), `localhost:9090` (metrics), `localhost:8081` (health)

### Try it out

```bash
# Create a stream and publish messages
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
nats kv add config --history=5
nats kv put config app.port "8080"
nats kv put config app.host "localhost"

# Query via cold storage:
curl -s localhost:8080/v1/kv/config/get/app.port | jq .
curl -s localhost:8080/v1/kv/config/keys | jq .
nats req nts.kv.config.get.app.port ""
nts-ctl kv keys config
```

### Object Store tiering

```bash
nats object add files
nats object put files /path/to/report.pdf --name=report.pdf

# Retrieve via cold storage:
curl -so report.pdf localhost:8080/v1/objects/files/get/report.pdf
curl -s localhost:8080/v1/objects/files/list | jq .
nats req nts.obj.files.info.report.pdf ""
nts-ctl obj list files
```

## Client Library (`pkg/nts`)

The [`pkg/nts`](https://pkg.go.dev/github.com/gftdcojp/nats-tiered-storage/pkg/nts) package provides a drop-in wrapper for JetStream with automatic cold storage fallback. It requires only `nats.go` — no other dependencies from this project.

```bash
go get github.com/gftdcojp/nats-tiered-storage/pkg/nts@latest
```

### API

| Type | Method | Description |
|---|---|---|
| `nts.New(Config)` | — | Create client with NATS conn + JetStream |
| `Client` | `GetMessage(ctx, stream, seq)` | Retrieve message (cold fallback) |
| `Client` | `KeyValue(ctx, bucket)` | Open KV store wrapper |
| `Client` | `ObjectStore(ctx, bucket)` | Open Object Store wrapper |
| `KVStore` | `Get(ctx, key)` | Get value (cold fallback) |
| `KVStore` | `Put(ctx, key, value)` | Put value (JetStream direct) |
| `KVStore` | `Delete(ctx, key)` | Delete key (JetStream direct) |
| `KVStore` | `Keys(ctx)` | List keys (cold fallback) |
| `KVStore` | `History(ctx, key)` | Get revisions (cold fallback) |
| `KVStore` | `Underlying()` | Access raw `jetstream.KeyValue` |
| `ObjStore` | `Get(ctx, name)` | Download object (cold fallback, chunk reassembly) |
| `ObjStore` | `GetInfo(ctx, name)` | Object metadata (cold fallback) |
| `ObjStore` | `Put(ctx, meta, reader)` | Upload object (JetStream direct) |
| `ObjStore` | `Delete(ctx, name)` | Delete object (JetStream direct) |
| `ObjStore` | `List(ctx)` | List objects (cold fallback) |
| `ObjStore` | `Underlying()` | Access raw `jetstream.ObjectStore` |

### How it works

```
client.KVStore.Get("app.port")
  │
  ├─ 1. Try JetStream KV Get
  │     └─ Found? → return immediately
  │
  └─ 2. Not found / deleted / purged
        └─ NATS Request → nts.kv.{bucket}.get.{key}
              └─ Sidecar retrieves from Memory / File / S3
                    └─ Return to caller (transparent)
```

### Example: migrate existing code

```go
// Before — standard JetStream
kv, _ := js.KeyValue(ctx, "config")
entry, _ := kv.Get(ctx, "app.port")       // fails if purged

// After — transparent cold fallback (2-line change)
client, _ := nts.New(nts.Config{NC: nc, JS: js})
kv, _ := client.KeyValue(ctx, "config")
entry, _ := kv.Get(ctx, "app.port")       // falls back to sidecar
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
      memory: { enabled: true, max_bytes: "256MB", max_age: "5m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data", max_age: "24h" }
      blob:   { enabled: true, bucket: "nats-tiered-archive", region: "us-east-1" }

  - name: "KV_config"
    consumer_name: "nts-archiver-kv"
    kv: { index_all_revisions: true }
    tiers:
      memory: { enabled: true, max_age: "10m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data" }

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
| `streams[].auto_mirror` | `true` | Auto-create Limits mirror for WorkQueue streams with existing consumers |
| `block.target_size` | `8MB` | Target block size before sealing |
| `block.max_linger` | `30s` | Max time to wait before sealing a partial block |
| `block.compression` | `s2` | Compression algorithm (`none`, `s2`) |
| `policy.eval_interval` | `30s` | How often to evaluate demotion policies |
| `tiers.memory.max_age` | — | Evict memory blocks older than this |
| `tiers.memory.max_bytes` | — | Evict oldest memory blocks when total exceeds this |
| `tiers.file.max_age` | — | Evict file blocks older than this |
| `tiers.blob.storage_class` | `STANDARD` | S3 storage class (`STANDARD`, `STANDARD_IA`, `GLACIER`) |

## Using with an Existing JetStream Environment

`nats-tiered-storage` is non-invasive — it connects as a standard NATS client and requires **no changes** to your existing `nats-server` or streams.

### Step 1: Identify your streams

```bash
nats stream ls
nats kv ls          # internal stream name: KV_{bucket}
nats object ls      # internal stream name: OBJ_{bucket}
```

### Step 2: Create a configuration file

```yaml
nats:
  url: "nats://your-nats-server:4222"
  # credentials_file: "/path/to/user.creds"
  # tls:
  #   ca_file: "/path/to/ca.pem"
  #   cert_file: "/path/to/cert.pem"
  #   key_file: "/path/to/key.pem"

streams:
  - name: "ORDERS"
    consumer_name: "nts-archiver"
    tiers:
      memory: { enabled: true, max_bytes: "256MB", max_age: "5m" }
      file:   { enabled: true, data_dir: "/var/lib/nts/data", max_age: "24h" }
      blob:   { enabled: true, bucket: "my-archive", region: "us-east-1" }

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

### Step 3: Deploy

```bash
# Binary
bin/nats-tiered-storage -config /path/to/config.yaml

# Docker (GHCR)
docker run -v /path/to/config.yaml:/etc/nts/config.yaml \
           -v /var/lib/nts:/var/lib/nts \
           ghcr.io/gftdcojp/nats-tiered-storage:0.3.0

# Kubernetes
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/sidecar-deployment.yaml
```

### How it works

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

- The sidecar creates its own **durable Pull Consumer** — it does not interfere with your existing consumers
- Messages remain in JetStream after the sidecar ACKs them — the sidecar's consumer is independent
- You can safely set `max_age` or `max_bytes` on JetStream to limit retention; the sidecar archives messages before they expire

#### WorkQueue streams

JetStream [WorkQueue retention](https://docs.nats.io/nats-concepts/jetstream/streams#retention-policies) streams only allow a single filter-less consumer. If the target stream uses WorkQueue retention and already has a consumer, the sidecar **automatically creates a Limits-retention mirror** (`NTS_MIRROR_{stream}`) and consumes from it instead. This is fully transparent — metadata and blocks still use the original stream name.

Auto-mirroring is enabled by default and can be disabled per stream:

```yaml
streams:
  - name: "TASKS"
    consumer_name: "nts-archiver"
    auto_mirror: false   # disable auto-mirror for this stream
```

### Typical use case

```
JetStream:  max_age=7d  (messages deleted after 7 days)
Sidecar:    write-through ingest → all enabled tiers at once
            eviction: Memory 5m → File 24h → S3 forever

Result: Hot data served from JetStream (fast),
        cold data served from sidecar (fallthrough reads)
```

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
| `POST` | `/v1/admin/demote/{stream}/{blockID}` | Evict a block from its hottest tier |
| `POST` | `/v1/admin/promote/{stream}/{blockID}` | Copy a block into the next hotter tier |

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

### Ingest (write-through)

```
nats-server JetStream
  → Pull Consumer (fetch batch)
    → Block Builder (accumulate to ~8MB or linger timeout)
      → Seal Block → Tier Controller.Ingest()
          → Write to Memory (if enabled)
          → Write to File   (if enabled)
          → Write to Blob   (if enabled)
        → Metadata Store.RecordBlock(tiers=[Memory,File,Blob])
          → ACK batch
```

All enabled tiers receive the block simultaneously. ACK only after all writes succeed.

### Eviction (automatic)

```
Memory [age > 5m / max_bytes exceeded] → evict from Memory (data remains in File + Blob)
File   [age > 24h / max_bytes exceeded] → evict from File (data remains in Blob)
Blob   [max_age exceeded] → delete from all tiers (permanent expiry)
```

Eviction only deletes from the hottest tier — no data copy needed (already in colder tiers via write-through).

### Retrieval (fallthrough reads)

```
Request → Metadata Lookup → Fallthrough tiers (hot → cold)
  → Try Memory  → hit? return
  → Try File    → hit? return
  → Try Blob    → hit? return (S3 Range Request)
  → All miss    → error
```

If Memory has evicted a block (LRU), the read automatically falls through to the next tier.

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
| `nts_mirror_streams_created_total` | Counter | Auto-created mirror streams for WorkQueue conflict avoidance |

### Health Checks

- **Liveness**: `GET :8081/healthz` — always returns 200 if the process is running
- **Readiness**: `GET :8081/readyz` — checks NATS connectivity, metadata store, and S3 (if configured)

## Testing

156+ tests covering all packages with race detection (plus stress tests behind build tags).

```bash
# All tests
go test -race -count=1 ./...

# Integration tests (embedded nats-server, no Docker required)
go test -race -count=1 -run TestIntegration ./internal/

# Durability tests (restart survival, CRC, tier transitions)
go test -race -count=1 -run TestDurability ./internal/

# Stress tests (high volume, concurrency, rapid tier migration)
go test -race -count=1 -tags stress -run TestStress ./internal/

# Coverage report
go test -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

| Category | Tests | Description |
|---|---|---|
| Block format | 9 | Encode/decode, builder, index, CRC checksums |
| Memory store | 8 | LRU eviction, MaxBlocks, concurrent access |
| File store | 15 | Put/Get, index lookup, corruption fallback, durability |
| Blob store | 12 | S3 mock, range requests, cache, concurrent race detection |
| Tier controller | 21 | Write-through ingest, eviction, fallthrough reads, promote, policy cycles, concurrency |
| Lifecycle/GC | 6 | Expiry, retention, partial failure, cancel |
| HTTP handlers | 12 | Status, blocks, messages, KV, Object Store, errors |
| Health/Metrics | 7 | Liveness, readiness, NATS/meta checks, Prometheus metrics |
| Client (`pkg/nts`) | 16 | Error helpers, KV/Obj sidecar fallback, embedded NATS |
| Integration | 3 | Full pipeline, KV Store, Object Store (end-to-end) |
| Durability | 7 | BoltDB restart, file restart, CRC, tier transitions, KV/Obj persistence |
| Stress | 6 | 10K msg ingest, 50-goroutine concurrency, rapid tier migration, 50K msg blocks |
| Mirror | 7 | WorkQueue auto-mirror resolution, naming, idempotency |
| Config | 7 | YAML parsing, validation, byte sizes, auto-mirror, tier retention |

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
│   ├── ingest/                 # JetStream consumer, pipeline, WorkQueue mirror, KV/Obj classifiers
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

## License

Apache License 2.0 — see [LICENSE](LICENSE).
