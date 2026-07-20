# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG RUST_VERSION=1.97.0
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.53
ARG DEBIAN_SNAPSHOT=20260701T000000Z

FROM ghcr.io/grpc-ecosystem/grpc-health-probe:${GRPC_HEALTH_PROBE_VERSION}@sha256:a732f1b3a737926c2902393809b344c9f293b62f7069dbd0614caebd298b2e8d AS health-probe

FROM rust:${RUST_VERSION}-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS chef
ARG DEBIAN_SNAPSHOT
RUN echo "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bookworm main" > /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install --yes --no-install-recommends clang cmake libclang-dev libprotobuf-dev libssl-dev pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN cargo install cargo-chef --locked --version 0.1.77

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --locked --release --package lake-cli --bin lake \
    && cp /src/target/release/lake /tmp/lake \
    && strip /tmp/lake

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
ARG DEBIAN_SNAPSHOT
RUN echo "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bookworm main" > /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 lake \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin lake \
    && install --directory --owner=65532 --group=65532 /tmp/lake-spill
COPY --from=builder /tmp/lake /usr/local/bin/lake
COPY --from=health-probe /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/lake"]
