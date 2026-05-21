# grpcserver

A high-throughput gRPC key-value service backed by RocksDB, with cursor-based pagination and gRPC reflection.

## Features

- **gRPC API** — `Put`, `Get`, `Delete`, `List`, `HealthCheck`, `GetStats`
- **Namespaced keys** — `(namespace, id)` pairs; prefix scans are isolated per namespace
- **Cursor pagination** — opaque, namespace-bounded page tokens
- **Embedded RocksDB** — multi-threaded handle, configurable compression (snappy/lz4/zstd)
- **gRPC reflection** — works with `grpcurl` out of the box
- **Structured logging** — `tracing` + `RUST_LOG` support
- **Graceful shutdown** — `SIGTERM` / `Ctrl-C` flush the database before exit

## Prerequisites

- Rust 1.74+ with `cargo`
- `protoc` (Protocol Buffers compiler) — `brew install protobuf` on macOS
- A C/C++ toolchain (the `rocksdb` crate compiles native code)

## Quick start

```bash
make build           # compile everything
make test            # 30 tests (unit + integration)
make run             # start the server on 0.0.0.0:50051
```

The server uses sensible defaults out of the box. To override them, copy `config.example.toml` to `config.toml` (or set `CONFIG_PATH=/path/to/your.toml`).

## Configuration

All keys are optional and fall back to defaults. See [`config.example.toml`](config.example.toml).

| Key | Default | Notes |
|---|---|---|
| `server.address` | `0.0.0.0:50051` | bind address |
| `server.log_level` | `info` | overridden by `RUST_LOG` if set |
| `server.default_page_size` | `100` | applied when `ListRequest.page_size == 0` |
| `server.max_page_size` | `1000` | upper bound on `ListRequest.page_size` |
| `db.path` | `./data/rocksdb` | RocksDB directory |
| `db.write_buffer_size` | 64 MiB | per-CF memtable size |
| `db.compression` | `snappy` | `none`, `snappy`, `lz4`, `zstd` |

## Service contract

See [`proto/kv.proto`](proto/kv.proto). Behaviour worth knowing:

- **Keys.** `namespace` and `id` must be non-empty UTF-8 and must not contain `\0`. Validation errors return `INVALID_ARGUMENT`.
- **`Put` semantics.** Upsert. `created_at_unix_ms` is preserved across overwrites; `updated_at_unix_ms` is refreshed on every write. `checksum` is `sha256(data)` hex-encoded.
- **`List` pagination.** Pass `next_page_token` from the previous response as `page_token` to fetch the next page. The token is opaque (base64url) and is validated against the requested namespace — tokens cannot be used to read other namespaces.
- **`page_size`.** `0` means "use server default". Values above `max_page_size` are silently capped.

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
```

## Development

```bash
make fmt           # rustfmt
make fmt-check     # rustfmt --check (CI)
make clippy        # clippy with -D warnings
make test          # unit + integration tests
make all           # fmt-check + clippy + test (CI default)
make release       # optimized build (LTO, codegen-units=1)
```

## Benchmark

`make bench` connects to an **already-running server** with a pool of concurrent gRPC clients and reports throughput plus latency percentiles (mean, p50, p95, p99, p99.9, max) for `Put`, `Get`, and `List`.

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

The benchmark does not clean up after itself. For repeatable PUT/LIST numbers, point at a fresh server (or set `BENCH_NAMESPACE` to a unique value such as `bench-$(date +%s)`) — leftover keys in the namespace skew LIST first-page latency.

## Architecture

| File | Responsibility |
|---|---|
| `proto/kv.proto` | Service contract |
| `build.rs` | Compiles the proto and emits the reflection descriptor set |
| `src/lib.rs` | Library root; includes generated proto code as `pb` |
| `src/config.rs` | TOML configuration with defaults |
| `src/error.rs` | Crate error type + `tonic::Status` conversion |
| `src/db.rs` | Thread-safe RocksDB wrapper with atomic stats counters |
| `src/server.rs` | `KvService` gRPC implementation |
| `src/main.rs` | Binary entrypoint, logging, signal handling |
| `tests/integration.rs` | End-to-end gRPC tests over loopback |

The DB layer takes `&self` everywhere and uses atomic counters, so a single `Arc<Database>` is shared across all gRPC handlers without locking on the hot path.
