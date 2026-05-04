# syntax=docker/dockerfile:1

# Pin by digest for immutability. Update via Dependabot/Renovate.

# -----------------------------------------------------------------------------
# Stage 1: build
# -----------------------------------------------------------------------------
FROM rust:1-bookworm@sha256:adab7941580c74513aa3347f2d2a1f975498280743d29ec62978ba12e3540d3a AS builder

WORKDIR /build

# Copy dependency manifests first for better layer caching.
COPY Cargo.toml Cargo.lock ./

# Pre-build dependencies only (no binary). Touch src so we have a placeholder.
ENV CARGO_TARGET_DIR=/target
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/target \
    mkdir -p src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release --features hooks,streaming && rm -rf src

# Copy real source and build the binary.
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/target \
    touch src/main.rs && cargo build --release --features hooks,streaming && \
    cp /target/release/helr /build/helr && \
    strip /build/helr

# -----------------------------------------------------------------------------
# Stage 2: runtime (distroless: no shell, no package manager, minimal libs)
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:e2d29aec8061843706b7e484c444f78fafb05bfe47745505252b1769a05d14f1

# Non-root user is set by the image (nonroot:65532).
# Mount config and state at runtime; do not bake secrets into the image.
WORKDIR /app

COPY --from=builder /build/helr /app/helr

ENTRYPOINT ["/app/helr"]
