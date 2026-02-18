# syntax=docker/dockerfile:1
# Multi-stage build: build in one stage, run in minimal distroless image.
# Pin base images by digest in production for reproducibility.

# -----------------------------------------------------------------------------
# Stage 1: build
# -----------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /build

# Copy dependency manifests first for better layer caching.
COPY Cargo.toml Cargo.lock ./

# Pre-build dependencies only (no binary). Touch src so we have a placeholder.
ENV CARGO_TARGET_DIR=/target
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/target \
    mkdir -p src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release --features hooks && rm -rf src

# Copy real source and build the binary.
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/target \
    touch src/main.rs && cargo build --release --features hooks && \
    cp /target/release/helr /build/helr

# -----------------------------------------------------------------------------
# Stage 2: runtime (distroless: no shell, no package manager, minimal libs)
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot

# Non-root user is set by the image (nonroot:65532).
# Mount config and state at runtime; do not bake secrets into the image.
WORKDIR /app

COPY --from=builder /build/helr /app/helr

ENTRYPOINT ["/app/helr"]
