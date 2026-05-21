# grpcserver

A high-throughput gRPC key-value service with a pluggable storage layer. Runs
either as a pure embedded RocksDB store, or as a Kafka-backed store with
RocksDB as a fallback buffer for outages. Ships with gRPC reflection,
cursor-based pagination, and a built-in throughput benchmark.

## Features

- **gRPC API** — `Put`, `Get`, `Delete`, `List`, `HealthCheck`, `GetStats`
- **Namespaced keys** — `(namespace, id)` pairs; prefix scans are isolated per namespace
- **Cursor pagination** — opaque, namespace-bounded page tokens
- **Pluggable storage** — `Storage` trait with two backends:
  - `RocksDbBackend` — embedded RocksDB, blocking calls offloaded to `tokio::task::spawn_blocking`
  - `KafkaBackend` (`--features kafka`) — single-topic, single-partition log with an in-memory `BTreeMap` cache rebuilt from offset 0 on startup
- **Automatic failover** — `StorageManager` picks the active backend per call; falls back from Kafka to RocksDB if Kafka becomes unreachable
- **gRPC reflection** — works with `grpcurl` out of the box
- **Structured logging** — `tracing` + `RUST_LOG` support
- **Graceful shutdown** — `SIGTERM` / `Ctrl-C` drain in-flight requests; RocksDB flushes before the process exits

## Prerequisites

- Rust 1.85+ with `cargo` (toolchain pinned in `Cargo.toml`)
- `protoc` (Protocol Buffers compiler) — `brew install protobuf` on macOS
- A C/C++ toolchain (the `rocksdb` crate compiles native code)

## Quick start

```bash
make build           # cargo build --all-targets
make test            # unit + integration tests
make run             # start the server on 0.0.0.0:50051 (RocksDB-only by default)
```

The server uses sensible defaults out of the box. To override them, edit
[`config.toml`](config.toml) (or point `CONFIG_PATH=/path/to/your.toml` at a
different file). With no file present the built-in defaults from
`src/config.rs` / `src/kafka_config.rs` are used.

To enable the Kafka backend you need the `kafka` cargo feature **and** a
reachable broker. The Docker image is built with `--features kafka` by default;
for local runs use `cargo run --release --features kafka` and either:

- Point `kafka.bootstrap_servers` at a broker you've started yourself, or
- Use `make docker-compose-up`, which bundles Kafka in KRaft mode.

## Operating modes

The service runs in one of three configurations chosen at startup. The mode is
logged on boot (`Server started on ... with Kafka enabled=<bool>`).

### 1. Pure RocksDB (default)

`kafka.enabled = false` (or the binary built without `--features kafka`).
Every `Put`/`Get`/`Delete`/`List` goes straight to embedded RocksDB. This is
the simplest mode — no external dependencies, durability is whatever RocksDB
gives you.

### 2. Kafka + RocksDB (healthy)

`kafka.enabled = true`, broker reachable at boot, `kafka` feature compiled in.

- Startup connects to Kafka (8 attempts over ~30s: `0, 1, 2, 4, 8, 8, 8, 8`s).
- On success, any leftover keys in RocksDB (buffered during a previous
  outage) are **drained into Kafka and then deleted from RocksDB** before
  the server begins accepting requests. This keeps the Kafka cache
  authoritative.
- The `KafkaBackend` consumes the topic from offset 0 to the high watermark
  and builds an in-memory `BTreeMap` cache. Reads serve from that cache
  (fast); writes produce to Kafka **and** update the cache locally.
- A background `AvailabilityManager` pings Kafka on `health_check_interval_s`.

### 3. Kafka degraded / unavailable

If Kafka is unreachable **at boot**, the server logs a warning and continues
in RocksDB-only mode for the rest of its lifetime. Restart once Kafka is
back to re-enable it.

If Kafka becomes unreachable **mid-run**, the health watcher latches the
`kafka_available` flag to `false` on the first failed probe. Subsequent
requests are served from RocksDB. The latch is **one-way**: even if Kafka
recovers, the process keeps using RocksDB until restart. This is
intentional — re-attaching would require replaying buffered RocksDB writes
into Kafka before serving reads from the (now-stale) cache, and that path
is out of scope.

While Kafka is the active backend, no writes hit RocksDB (Kafka is
canonical). While RocksDB is the active backend (degraded mode), writes
accumulate there and will be drained into Kafka the next time the process
starts and finds Kafka reachable.

> **Caveat — single-writer assumption.** `KafkaBackend` produces to Kafka
> and updates its local cache in-process. It does **not** run a consumer
> thread that reapplies committed records. Running more than one instance
> against the same topic would let each replica's cache diverge. Run a
> single writer per topic.

## Configuration

All keys are optional and fall back to defaults. The canonical config —
including inline documentation for every field — lives in
[`config.toml`](config.toml).

### `[server]`

| Key | Default | Notes |
|---|---|---|
| `address` | `0.0.0.0:50051` | bind address |
| `log_level` | `info` | overridden by `RUST_LOG` if set |
| `default_page_size` | `100` | applied when `ListRequest.page_size == 0` |
| `max_page_size` | `1000` | upper bound on `ListRequest.page_size` |

### `[db]` (RocksDB)

| Key | Default | Notes |
|---|---|---|
| `path` | `./data/rocksdb` | RocksDB directory |
| `max_open_files` | `-1` | `-1` = unlimited |
| `write_buffer_size` | 64 MiB | per-CF memtable size |
| `compression` | `snappy` | `none`, `snappy`, `lz4`, `zstd` |
| `create_if_missing` | `true` | |

### `[kafka]`

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | master switch; with `false` the server runs as pure RocksDB |
| `bootstrap_servers` | `localhost:9092` | comma-separated `host:port,host:port` |
| `topic` | `kv_entries` | created if missing; single partition for ordering |
| `replication_factor` | `1` | applied only when the topic does not yet exist |
| `health_check_interval_s` | `10` | how often the watcher probes Kafka |
| `drain_interval_s` | `5` | reserved; the only drain currently runs once at startup |
| `message_max_bytes` | `10485760` (10 MiB) | produce/fetch cap |

## Service contract

See [`proto/kv.proto`](proto/kv.proto). Behaviour worth knowing:

- **Keys.** `namespace` and `id` must be non-empty UTF-8 and must not contain `\0`. Validation errors return `INVALID_ARGUMENT`.
- **`Put` semantics.** Upsert. `created_at_unix_ms` is preserved across overwrites; `updated_at_unix_ms` is refreshed on every write. `checksum` is `sha256(data)` hex-encoded. `Put` always reads before writing (to preserve `created_at_unix_ms`), so every `Put` costs one read + one write.
- **`List` pagination.** Pass `next_page_token` from the previous response as `page_token` to fetch the next page. The token is opaque (base64url) and is validated against the requested namespace — tokens cannot be used to read other namespaces.
- **`page_size`.** `0` means "use server default". Values above `max_page_size` are silently capped.
- **Error mapping.** `INVALID_ARGUMENT` for bad input / bad page tokens; `UNAVAILABLE` for Kafka errors (so clients can retry with backoff); `INTERNAL` for storage / encode / decode failures.
- **`GetStats`.** Counters track user-visible operations at the dispatch layer (the `StorageManager`), so they reflect what was served regardless of which backend handled the request.

## Docker

### Quick start with Docker Compose

The bundled compose stack runs Kafka (KRaft mode, no ZooKeeper) and the gRPC
server in one shot:

```bash
make docker-compose-up      # build + start in detached mode
make docker-compose-logs    # tail logs
make docker-compose-down    # stop everything
```

The gRPC server is available at `localhost:50051` and Kafka at
`localhost:9092`. The grpc-server container waits for Kafka to report healthy
before starting, so the boot-time connection attempt should succeed first try.

The image is built with `--features kafka` and reads `./config.toml` from a
read-only bind mount.

### Build / push the image directly

```bash
make docker-build                                        # builds grpcserver:latest
IMAGE_REGISTRY=myregistry.io:5000 make docker-push       # tags + pushes
docker build -t myregistry/grpcserver:v1.0.0 .           # custom tag
```

### Run tests in Docker

```bash
make docker-test     # runs `cargo test --features kafka` inside a Rust container
```

## Example calls

With `grpcurl` (reflection is enabled, so no `.proto` flag needed):

```bash
# Put
grpcurl -plaintext -d '{"id":"k1","namespace":"demo","data":"aGVsbG8="}' \
  localhost:50051 kv.KvService/Put

# Get
grpcurl -plaintext -d '{"id":"k1","namespace":"demo"}' \
  localhost:50051 kv.KvService/Get

# Paginate
grpcurl -plaintext -d '{"namespace":"demo","page_size":50}' \
  localhost:50051 kv.KvService/List

# Health
grpcurl -plaintext localhost:50051 kv.KvService/HealthCheck

# Stats
grpcurl -plaintext localhost:50051 kv.KvService/GetStats
```

## Development

```bash
make fmt           # rustfmt
make fmt-check     # rustfmt --check (CI)
make clippy        # clippy with -D warnings, --all-features
make test          # unit + integration tests
make all           # fmt-check + clippy + test (CI default)
make release       # optimized build (LTO, codegen-units=1)
```

## Benchmark

`make bench` connects to an **already-running server** with a pool of
concurrent gRPC clients and reports throughput plus latency percentiles
(mean, p50, p95, p99, p99.9, max) for `Put`, `Get`, and `List`.

```bash
# Terminal 1: start the server (release build recommended)
cargo run --release

# Terminal 2: run the benchmark
make bench
```

Tune via env vars (defaults shown):

```bash
BENCH_ENDPOINT=http://127.0.0.1:50051 \
BENCH_NAMESPACE=bench \
BENCH_DURATION_S=10 \
BENCH_WARMUP_S=2 \
BENCH_CONCURRENCY=32 \
BENCH_VALUE_SIZE=256 \
BENCH_SEED_COUNT=10000 \
BENCH_LIST_PAGE_SIZE=100 \
  make bench
```

The benchmark does not clean up after itself. For repeatable PUT/LIST
numbers, point at a fresh server (or set `BENCH_NAMESPACE` to a unique value
such as `bench-$(date +%s)`) — leftover keys in the namespace skew LIST
first-page latency.

## Architecture

```
                ┌─────────────────────────┐
   gRPC ───────►│       KvServiceImpl     │   (src/server.rs)
                │  Arc<dyn Storage>       │
                └────────────┬────────────┘
                             │  Storage trait
                ┌────────────▼────────────┐
                │      StorageManager     │   (src/storage/mod.rs)
                │  picks active backend   │
                │  tracks user counters   │
                └─────┬───────────────┬───┘
                      │               │
        ┌─────────────▼───┐      ┌────▼────────────┐
        │  RocksDbBackend │      │   KafkaBackend  │
        │  (always open)  │      │  (cfg=kafka)    │
        └─────────────────┘      └─────────────────┘
                                       ▲
                                       │ pings every health_check_interval_s
                                ┌──────┴──────────────┐
                                │ AvailabilityManager │
                                │  latch-down only    │
                                └─────────────────────┘
```

| File | Responsibility |
|---|---|
| `proto/kv.proto` | Service contract |
| `build.rs` | Compiles the proto and emits the reflection descriptor set |
| `src/lib.rs` | Library root; includes generated proto code as `pb` |
| `src/config.rs` | TOML configuration with defaults |
| `src/kafka_config.rs` | Kafka-specific config section |
| `src/error.rs` | Crate error type + `tonic::Status` mapping (`UNAVAILABLE` for Kafka) |
| `src/stats.rs` | Atomic operation counters + `StatsSnapshot` |
| `src/storage/mod.rs` | `Storage` trait + `StorageManager` orchestrator + startup drain |
| `src/storage/rocksdb.rs` | `RocksDbBackend`; offloads blocking calls to `spawn_blocking` |
| `src/storage/kafka.rs` | `KafkaBackend`; single-topic log + in-memory `BTreeMap` cache |
| `src/storage/availability.rs` | Latch-down health watcher for Kafka |
| `src/server.rs` | `KvService` gRPC implementation |
| `src/main.rs` | Binary entrypoint, logging, signal handling |
| `benches/throughput.rs` | Concurrent benchmark client (uses `harness = false`) |
| `tests/integration.rs` | End-to-end gRPC tests over loopback (RocksDB-only) |

Every method on `Storage` takes `&self` and the trait is shared as
`Arc<dyn Storage>`. Backends keep their own internal synchronization
(RocksDB is thread-safe; `KafkaBackend` uses an `RwLock<BTreeMap>` around
the cache), so the gRPC handlers never lock on the hot path.
