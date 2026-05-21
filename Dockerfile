# Multi-stage build for gRPC server
# ============================================================
# Stage 1: Build
# ============================================================
FROM rust:1.85-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libclang-dev \
    llvm-dev \
    g++ \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source code and build release binary with Kafka feature enabled
COPY . .
RUN cargo build --release --features kafka

# ============================================================
# Stage 2: Runtime
# ============================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies. `netcat-openbsd` powers the healthcheck.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary
COPY --from=builder /app/target/release/grpcserver /app/grpcserver

# Expose default port
EXPOSE 50051

# Health check: TCP probe on the gRPC port. The gRPC server itself exposes a
# HealthCheck RPC but checking it from inside the container would require
# grpcurl, which we deliberately don't ship.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
  CMD nc -z 127.0.0.1 50051 || exit 1

# Run
ENTRYPOINT ["/app/grpcserver"]
CMD [""]
