CARGO ?= cargo

.PHONY: help all build release run test test-lib test-integration bench check clippy fmt fmt-check clean docs install-tools

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} \
	      /^[a-zA-Z_-]+:.*?##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: fmt-check clippy test ## Lint, format-check and run all tests (CI default)

build: ## Compile debug binary and tests
	$(CARGO) build --all-targets

release: ## Compile release binary
	$(CARGO) build --release

run: ## Run the server (debug build)
	$(CARGO) run

test: ## Run all tests (unit + integration; benches excluded)
	$(CARGO) test --lib --bins --tests --no-fail-fast

test-lib: ## Run only library unit tests
	$(CARGO) test --lib

test-integration: ## Run only integration tests
	$(CARGO) test --test '*'

bench: ## Throughput benchmark against a running server (start with `cargo run --release` first). Env: BENCH_ENDPOINT BENCH_NAMESPACE BENCH_DURATION_S BENCH_WARMUP_S BENCH_CONCURRENCY BENCH_VALUE_SIZE BENCH_SEED_COUNT BENCH_LIST_PAGE_SIZE
	$(CARGO) bench --bench throughput

check: ## Type-check without building artifacts
	$(CARGO) check --all-targets

clippy: ## Lint with clippy; warnings are errors
	$(CARGO) clippy --all-targets --all-features -- -D warnings

fmt: ## Apply rustfmt
	$(CARGO) fmt --all

fmt-check: ## Verify rustfmt with no changes
	$(CARGO) fmt --all -- --check

clean: ## Remove build artifacts
	$(CARGO) clean

docs: ## Build and open API docs
	$(CARGO) doc --no-deps --open

install-tools: ## Install rustfmt and clippy via rustup
	rustup component add rustfmt clippy
