# syntax=docker/dockerfile:1

# Pin by digest for immutability. Update via Dependabot/Renovate.

# -----------------------------------------------------------------------------
# Stage 1: build
# -----------------------------------------------------------------------------
FROM rust:1-trixie@sha256:e259494b14672950a909b4c54527e19d29cae6ebf980672114c8bb85e5594e4a AS builder

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
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:e792ab3d241a468a4fd7519ddbbebe66b49b5f365771716ea688ad40b6c6f1c2

# Non-root user is set by the image (nonroot:65532).
# Mount config and state at runtime; do not bake secrets into the image.
WORKDIR /app

COPY --from=builder /build/helr /app/helr

ENTRYPOINT ["/app/helr"]
