# Multi-stage Dockerfile for sv2-p2pool.
#
# Stage 1 (builder): pin to the same Rust version as rust-toolchain.toml.
# Builds the release binary with all submodule path deps.
#
# Stage 2 (runtime): minimal Debian slim with the runtime libs the
# binary needs (libssl, libgcc, capnproto runtime). Ships the
# binary + a non-root user.
#
# Build:
#   docker build -t sv2-p2pool:latest .
#
# Run (with bind-mounted configs):
#   docker run --rm -v $PWD/deploy/config:/etc/sv2-p2pool:ro \
#              -p 34254:34254 -p 34264:34264 -p 9000:9000 \
#              sv2-p2pool:latest

FROM rust:1.88-bookworm AS builder

# bitcoin-capnp-types build script needs the capnp binary + libcapnp-dev.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        capnproto libcapnp-dev pkg-config libssl-dev \
        build-essential clang llvm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
# Copy everything (submodules included). The source layout has
# vendor/sv2-apps and vendor/p2poolv2 as path deps; without those
# submodule trees we can't resolve.
COPY . .

# --locked: refuse drift from Cargo.lock. --release: optimised binary.
# --frozen would also reject network access; we leave that off because
# crates.io fetches still happen.
RUN cargo build --release --locked --bin sv2-p2pool

# ---- runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates libssl3 libcapnp-1.0.1 \
    && rm -rf /var/lib/apt/lists/* && \
    useradd --system --home-dir /var/lib/sv2-p2pool --shell /usr/sbin/nologin sv2-p2pool && \
    mkdir -p /etc/sv2-p2pool /var/lib/sv2-p2pool /var/log/sv2-p2pool && \
    chown sv2-p2pool:sv2-p2pool /var/lib/sv2-p2pool /var/log/sv2-p2pool

COPY --from=builder /build/target/release/sv2-p2pool /usr/local/bin/sv2-p2pool

USER sv2-p2pool
WORKDIR /var/lib/sv2-p2pool

# Mining + JDS + metrics ports (match deploy/config/pool.example.toml).
EXPOSE 34254 34264 9000

ENTRYPOINT ["/usr/local/bin/sv2-p2pool"]
CMD [ \
    "--config", "/etc/sv2-p2pool/pool.toml", \
    "--p2pool-config", "/etc/sv2-p2pool/p2pool.toml", \
    "--metrics-addr", "0.0.0.0:9000" \
]
