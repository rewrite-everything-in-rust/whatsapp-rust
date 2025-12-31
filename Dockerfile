FROM rust:slim-bookworm AS base

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

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
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN groupadd -r appgroup && useradd -r -g appgroup appuser

COPY --from=builder /app/target/release/whatsapp-rust /app/whatsapp-rust
RUN chown appuser:appgroup /app/whatsapp-rust

USER appuser
VOLUME ["/app/data"]
ENV RUST_LOG=info
WORKDIR /app/data

ENTRYPOINT ["/app/whatsapp-rust"]
