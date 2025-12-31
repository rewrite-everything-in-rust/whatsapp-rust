# Using Alpine for a smaller footprint
FROM rust:alpine AS base

# Install build dependencies
# build-base is needed for compiling C dependencies (like sqlite3-sys bundled)
RUN apk add --no-cache build-base

RUN cargo install cargo-chef 
WORKDIR /app

# Planner Stage
FROM base AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Cacher Stage
FROM base AS cacher 
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Builder Stage
FROM base AS builder
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN cargo build --release --locked

# Runtime Stage
FROM alpine:latest

# Install runtime dependencies
# ca-certificates for SSL/TLS, tzdata for timezones, tini for signal handling
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    tini \
    libgcc

WORKDIR /app

# Create a non-root user
# RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy the binary
COPY --from=builder /app/target/release/whatsapp-rust /app/whatsapp-rust
# RUN chown appuser:appgroup /app/whatsapp-rust

# USER appuser
VOLUME ["/app/data"]
ENV RUST_LOG=info
WORKDIR /app/data

# Use tini as the entrypoint to handle signals correctly (like SIGTERM)
ENTRYPOINT ["/sbin/tini", "--", "/app/whatsapp-rust"]
