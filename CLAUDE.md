# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Use the Makefile for everything; targets shell out to plain `cargo` commands. Important ones:

- `make build` — `cargo build --all-targets`. Requires `protoc` on `PATH` (the `build.rs` compiles `proto/kv.proto` and emits a reflection descriptor set to `$OUT_DIR/kv_descriptor.bin`, which `src/lib.rs` includes via `include_bytes!`).
- `make test` — runs unit tests (in `#[cfg(test)]` modules under `src/`) and the integration tests in `tests/integration.rs`. Filter with `cargo test <name>` directly.
- `make clippy` — `cargo clippy --all-targets --all-features -- -D warnings`. CI-strict; keep it clean — `--all-features` means the Kafka path is linted too.
- `make fmt` / `make fmt-check` — rustfmt apply / verify.
- `make all` — `fmt-check` + `clippy` + `test` (the CI pipeline).
- `make run` — boots the server with default features (no Kafka). With no `config.toml` present it uses built-in defaults; otherwise it loads `$CONFIG_PATH` or `./config.toml`. The checked-in `config.toml` doubles as documentation — every key has an inline comment.
- `make bench` — runs `benches/throughput.rs` against an already-running server. The bench target is registered in `Cargo.toml` with `harness = false` because the file owns its own `#[tokio::main]`. Start the server in another shell first (`cargo run --release`).

### Feature flags

- `default = []`. Building without flags gives a pure RocksDB binary; the `kafka` module compiles to a stub via `#[cfg(feature = "kafka")]`.
- `kafka = ["dep:rskafka", "dep:chrono"]`. Required for the Kafka backend. The Docker image and `make docker-test` enable it; local `cargo run` / `cargo test` do not.

### Docker

- `make docker-build` — multi-stage build in `Dockerfile` (`rust:1.85-slim` builder → `debian:bookworm-slim` runtime). Always built with `--features kafka`.
- `make docker-compose-up` — KRaft-mode Kafka + gRPC server, both in detached mode. Server at `localhost:50051`, Kafka at `localhost:9092`. The grpc-server `depends_on` Kafka's healthcheck.
- `make docker-compose-down` / `make docker-compose-logs` — stop / tail.
- `make docker-test` — runs `cargo test --features kafka` inside a rust:1.85-slim container with `protoc` installed.
- `IMAGE_REGISTRY=myregistry.io:5000 make docker-push` — tags and pushes `grpcserver:latest`.

## Architecture

Single gRPC binary (`grpcserver`) implementing a namespaced KV store with a
pluggable storage layer. Library + binary split: the public surface lives in
`src/lib.rs` so `tests/integration.rs` and `benches/throughput.rs` can drive
the service without going through the binary.

### Storage dispatch model

```
KvServiceImpl ─► Arc<dyn Storage> ─► StorageManager
                                       │
                                       ├── always: Arc<RocksDbBackend>
                                       └── if cfg+enabled+reachable:
                                              Arc<KafkaBackend>
                                              + AvailabilityManager
```

- `Storage` is a `Send + Sync` async trait with `get`, `put`, `delete`,
  `scan_prefix`, and `stats`. It is **used via `dyn Storage`**, which is
  why `#[async_trait::async_trait]` is still required on every impl —
  native AFIT in stable Rust does not support trait objects. The
  generated `KvService` trait in `tonic` 0.12 also still emits
  `#[async_trait]`, so the dependency stays regardless.
- `StorageManager::active_backend(&self) -> &dyn Storage` picks the
  current backend per call based on `AvailabilityManager::kafka_available()`.

### Module layout

- `proto/kv.proto` — service contract (`package kv`, service `KvService`). Six RPCs: `Put`, `Get`, `Delete`, `List`, `HealthCheck`, `GetStats`. Regenerate by re-running `cargo build` after edits.
- `build.rs` — invokes `tonic_build::configure().file_descriptor_set_path(...).compile_protos(...)`. The descriptor set is what powers gRPC reflection at runtime.
- `src/lib.rs` — re-exports the public surface and contains `pub mod pb { tonic::include_proto!("kv"); }` plus `FILE_DESCRIPTOR_SET`.
- `src/config.rs` — TOML config with `#[serde(default)]` everywhere; `Config::load()` returns defaults when the file is missing. `Config::parse` (not `from_str`, to avoid the `FromStr` clippy lint) handles raw strings.
- `src/kafka_config.rs` — the `[kafka]` section. Fields are always present in the deserialized struct; the `kafka.enabled` flag is what gates the runtime behaviour.
- `src/error.rs` — single crate error enum with `From` impls for `rocksdb::Error`, `prost::EncodeError`, `prost::DecodeError`. `From<Error> for tonic::Status` maps `InvalidArgument`/`InvalidPageToken` → `INVALID_ARGUMENT`, `Kafka` → `UNAVAILABLE` (so clients retry with backoff), everything else → `INTERNAL`. The `Kafka` variant is `#[cfg(feature = "kafka")]`.
- `src/stats.rs` — `Stats` (four `AtomicU64`) + `StatsSnapshot` (the public DTO). Counters are tracked at whichever level "owns" them — see the Conventions section below.
- `src/storage/mod.rs` — the `Storage` trait, `StorageManager`, and the helper functions that boot Kafka (`connect_kafka_with_retry` — 8 attempts at `0,1,2,4,8,8,8,8` seconds) and drain leftover RocksDB entries (`drain_rocksdb_into_kafka`). `StorageManager` has two `new` impls gated on `kafka` feature; the non-kafka one just wraps RocksDB.
- `src/storage/rocksdb.rs` — `RocksDbBackend` wraps `DBWithThreadMode<MultiThreaded>` in an `Arc`. Every async method offloads to `tokio::task::spawn_blocking` because RocksDB calls are blocking. Owns its own `Stats` (used only when the backend stands alone; see Conventions).
- `src/storage/kafka.rs` — `KafkaBackend`. Single topic, single partition (for total ordering). Records use `key = namespace\0id` and `value = Some(encoded DataEntry)` for puts or `None` for tombstones. On `open`, replays the topic from offset 0 to high watermark into an in-memory `BTreeMap` (`RwLock<BTreeMap<Vec<u8>, Vec<u8>>>`). Writes produce to Kafka then update the cache.
- `src/storage/availability.rs` — `AvailabilityManager`. `disabled()` is a permanently-off variant; `watching()` spawns a background task that pings `KafkaBackend::check_connection` on `health_check_interval_s` and flips an `AtomicBool` to false on the first failure. **Latch-down only** — no recovery without process restart. The task keeps running (idly) after latching so the `JoinHandle` stays valid.
- `src/server.rs` — `KvServiceImpl` implementing the generated `KvService` trait. Holds `storage: Arc<dyn Storage>`. Keys are `{namespace}\0{id}` with a `KEY_SEP = 0` constant; `namespace` and `id` are validated to be non-empty and free of `\0`. Pagination uses base64url-encoded full keys as page tokens, and the token is checked to start with the requested namespace's prefix before use (prevents cross-namespace cursor reuse). The impl is annotated `#[tonic::async_trait]` because the tonic codegen still requires it.
- `src/main.rs` — `Config::load` → `tracing-subscriber` with `RUST_LOG` precedence → `StorageManager::new` → `Server::serve_with_shutdown` listening for `SIGTERM` / `Ctrl-C`. After the signal fires, there is an explicit `5s` sleep before forcing termination so in-flight requests can drain.
- `benches/throughput.rs` — standalone concurrent benchmark client. Custom `main()` (`harness = false` in `Cargo.toml`). Talks to a running server over the wire.
- `tests/integration.rs` — spawns the server on an ephemeral port (`std::net::TcpListener::bind("127.0.0.1:0")` → drop → re-bind via `serve`), then drives it through a real `KvServiceClient` against `RocksDbBackend` (Kafka is not exercised here). The `TempDir` backing RocksDB is moved into the spawned task so it drops *after* the `Server` (and thus after the database), avoiding "directory deleted before flush" races.

### Lifecycle (`StorageManager::new`)

1. Open RocksDB unconditionally.
2. If `kafka.enabled == false` → return a manager with `kafka = None`, `availability = disabled()`. Same path is taken when the `kafka` feature is off entirely.
3. Otherwise call `connect_kafka_with_retry`. On failure → warn and return a RocksDB-only manager (latched off; needs restart to retry).
4. On success → call `drain_rocksdb_into_kafka`, which iterates every RocksDB entry, produces it to Kafka, then deletes it from RocksDB **via the `Storage` trait** so the deletes counter is updated.
5. Spawn `AvailabilityManager::watching` and return.

### Failure modes

- **Kafka unreachable at boot.** Logs `Kafka unreachable at startup; serving from RocksDB. Restart once Kafka is reachable to enable it.` Server stays up in RocksDB-only mode forever.
- **Kafka becomes unreachable mid-run.** Watcher logs `Kafka health check failed: ... Failing over to RocksDB. Restart to re-enable Kafka.` and flips the flag. From this point `active_backend()` returns `&RocksDbBackend`. Writes accumulate in RocksDB and will be drained into Kafka on the next successful boot.
- **Re-enable Kafka.** Process restart only. The latch is one-way by design; re-attaching mid-run would require replaying the RocksDB buffer into Kafka before serving Kafka reads, and that path is intentionally out of scope.

## Conventions worth knowing

- **`Storage` is used via `dyn` — `async-trait` is mandatory.** Don't try to swap to native AFIT for the trait; `Arc<dyn Storage>` in `KvServiceImpl` and `&dyn Storage` from `StorageManager::active_backend` need it. Tonic 0.12's codegen also still emits `#[async_trait]` on `KvService`. Removing the direct `async-trait` dep wouldn't even shrink the build graph (tonic pulls it transitively).
- **Stats ownership.** Each backend has its own `Stats` field, but `StorageManager` keeps the **user-visible** counters at the dispatch layer in `StorageManager::counters`. `KafkaBackend::stats()` returns `StatsSnapshot::default()` deliberately — when used through the manager, the manager's numbers are what `GetStats` returns. `RocksDbBackend::stats()` is meaningful only when RocksDB is used directly (e.g. in `tests/integration.rs`).
- **Delete probe doesn't advance reads.** `RocksDbBackend::delete` uses the inner rocksdb handle for its existence probe, so the public `reads` counter does *not* advance. There is a unit test pinning this behaviour (`stats_track_operations`) — if you change it, update the test.
- **`Put` reads before writing.** Necessary to preserve `created_at_unix_ms` across overwrites. Each `Put` therefore costs one read + one write; this shows up in the stats counters as well as in the benchmark.
- **Page size resolution.** `page_size == 0` → server default; values above `max_page_size` are silently capped (not rejected). Pinned in `list_applies_default_and_max_page_size` integration test.
- **Page token security.** Tokens are base64url-encoded full storage keys. The server requires the decoded token to start with `{namespace}\0` before using it, so a malicious client cannot supply a token to read another namespace.
- **Single-writer Kafka assumption.** `KafkaBackend` produces and then mutates its local `BTreeMap` cache. It does **not** run a consumer that reapplies committed records. Running two replicas against the same topic would let their caches diverge. If you ever add HA, you need a consumer loop and a different cache invariant.
- **Kafka latch-down is intentional.** Don't add automatic re-attach without first deciding how to drain RocksDB-buffered writes back into Kafka (and how to handle the gap where the Kafka cache is stale relative to RocksDB).
- **`drain_interval_s` is currently unused.** The config field exists but the only drain runs once at startup. Don't take its presence as evidence that periodic drains are wired up.
- **Bench target needs `harness = false`.** `benches/throughput.rs` ships its own `#[tokio::main]`. The `[[bench]]` entry in `Cargo.toml` with `harness = false` is load-bearing — without it cargo would compile the file under libtest and silently never run the benchmark.
- **Kafka error → `UNAVAILABLE`.** The `From<Error> for tonic::Status` impl maps `Error::Kafka` to `Status::unavailable`. This is deliberate so gRPC clients with retry middleware back off and retry, instead of treating it as `INTERNAL` and giving up.
