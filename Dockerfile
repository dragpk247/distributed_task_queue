# ==============================================================================
# Multi-stage Dockerfile for distributed_task_queue
# Builds a minimal, highly secure production container image.
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Build & Compilation Environment
# ------------------------------------------------------------------------------
FROM rust:1.80-alpine AS builder

# Install build dependencies required for compiling Rust binaries statically
RUN apk add --no-cache musl-dev

WORKDIR /app

# Cache dependencies by copying Cargo files first
COPY Cargo.toml ./

# Create dummy source files to pre-compile dependencies
RUN mkdir -p src && \
    echo "pub mod protocol; pub mod engine; pub mod aof; pub mod server; pub mod http_server;" > src/lib.rs && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/distributed_task_queue*

# Copy the actual source code and test directories
COPY src ./src
COPY tests ./tests

# Build the optimized release binary with musl libc
RUN cargo build --release

# ------------------------------------------------------------------------------
# Stage 2: Minimal Runtime Environment
# ------------------------------------------------------------------------------
FROM alpine:3.20

# Create non-root user and persistent data directory
RUN addgroup -S appgroup && adduser -S appuser -G appgroup && \
    mkdir -p /data && chown -R appuser:appgroup /data

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/distributed_task_queue /usr/local/bin/distributed_task_queue

# Set permissions
USER appuser

# Persistence volume for AOF logs
VOLUME ["/data"]

# Expose Redis-compatible port and HTTP metrics/dashboard port
EXPOSE 6379 9090

# Set default entrypoint with configurable persistence path in /data
ENTRYPOINT ["/usr/local/bin/distributed_task_queue"]
CMD ["--bind", "0.0.0.0:6379", "--aof", "/data/queue_persistence.aof"]
