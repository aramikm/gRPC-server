# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Use the Makefile for everything; targets shell out to plain `cargo` commands. Important ones:

- `make build` — `cargo build --all-targets`. Requires `protoc` on `PATH` (the `build.rs` compiles `proto/kv.proto` and emits a reflection descriptor set to `$OUT_DIR/kv_descriptor.bin`, which `src/lib.rs` includes via `include_bytes!`).
- `make test` — runs unit tests (in `#[cfg(test)]` modules under `src/`) and the integration tests in `tests/integration.rs`. Filter with `cargo test <name>` directly.
- `make clippy` — `cargo clippy --all-targets --all-features -- -D warnings`. CI-strict; keep it clean.
- `make fmt` / `make fmt-check` — rustfmt apply / verify.
- `make all` — `fmt-check` + `clippy` + `test` (the CI pipeline).
- `make run` — boots the server. With no `config.toml` present it uses built-in defaults; otherwise it loads `$CONFIG_PATH` or `./config.toml`. See `config.example.toml`.

## Architecture

Single gRPC binary (`opentest`) implementing a namespaced key-value store on top of RocksDB. Library + binary split: most logic lives in `src/lib.rs` so it can be exercised from `tests/integration.rs` without going through the binary.

Module layout:

- `proto/kv.proto` — service contract (`package kv`, service `KvService`). Six RPCs: `Put`, `Get`, `Delete`, `List`, `HealthCheck`, `GetStats`. Regenerate by re-running `cargo build` after edits.
- `build.rs` — invokes `tonic_build::configure().file_descriptor_set_path(...).compile_protos(...)`. The descriptor set is what powers gRPC reflection at runtime.
- `src/lib.rs` — re-exports the public surface and contains `pub mod pb { tonic::include_proto!("kv"); }` plus `FILE_DESCRIPTOR_SET`.
- `src/config.rs` — TOML config with `#[serde(default)]` everywhere; `Config::load()` returns defaults when the file is missing. `Config::parse` (not `from_str`, to avoid the `FromStr` clippy lint) handles raw strings.
- `src/error.rs` — single crate error enum with `From` impls for `rocksdb::Error`, `prost::EncodeError`, `prost::DecodeError`, and a `From<Error> for tonic::Status` that maps each variant to the appropriate gRPC status code.
- `src/db.rs` — `Database` wraps `DBWithThreadMode<MultiThreaded>` and `Stats` (four `AtomicU64`). All methods take `&self`, so a single `Arc<Database>` is shared across handlers with no extra locking. `scan_prefix(prefix, start_key, limit)` is the pagination primitive.
- `src/server.rs` — `KvServiceImpl` implementing the generated `KvService` trait. Keys are `{namespace}\0{id}` with a `KEY_SEP = 0` constant; `namespace` and `id` are validated to be non-empty and free of `\0`. Pagination uses base64url-encoded full keys as page tokens, and the token is checked to start with the requested namespace's prefix before use (prevents cross-namespace cursor reuse).
- `src/main.rs` — config load → `tracing-subscriber` with `RUST_LOG` precedence → spawn `Server` with reflection + the KV service, with `serve_with_shutdown` listening for `SIGTERM` / `Ctrl-C`.
- `tests/integration.rs` — spawns the server on an ephemeral port (`std::net::TcpListener::bind("127.0.0.1:0")` → drop → re-bind via `serve`), then drives it through a real `KvServiceClient`. The `TempDir` backing RocksDB is moved into the spawned task so it drops *after* the `Server` (and thus after the `Database`), avoiding "directory deleted before flush" races.

## Conventions worth knowing

- **Stats and the `delete` probe.** `Database::delete` calls the inner rocksdb handle directly for its existence probe, so the public `reads` counter does *not* advance. There is a unit test pinning this behaviour — if you change it, update the test.
- **`Put` reads before writing.** Necessary to preserve `created_at_unix_ms` across overwrites. Each `Put` therefore costs one read + one write; this shows up in the stats counters.
- **Page size resolution.** `page_size == 0` → server default; values above `max_page_size` are silently capped (not rejected). Pinned in `list_applies_default_and_max_page_size` integration test.
- **Page token security.** Tokens are base64url-encoded full storage keys. The server requires the decoded token to start with `{namespace}\0` before using it, so a malicious client cannot supply a token to read another namespace.
