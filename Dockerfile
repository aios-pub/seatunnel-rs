# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0

# ===== Builder Stage =====
FROM rust:1.82-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    pkg-config \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY seatunnel-api/ ./seatunnel-api/
COPY seatunnel-config/ ./seatunnel-config/
COPY seatunnel-formats/ ./seatunnel-formats/
COPY seatunnel-engine/ ./seatunnel-engine/
COPY seatunnel-connectors/ ./seatunnel-connectors/
COPY seatunnel-transforms/ ./seatunnel-transforms/
COPY seatunnel-cli/ ./seatunnel-cli/
COPY seatunnel-macros/ ./seatunnel-macros/

RUN cargo build --release --bin seatunnel

# ===== Runtime Stage =====
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r seatunnel && useradd -r -g seatunnel seatunnel

WORKDIR /opt/seatunnel

COPY --from=builder /build/target/release/seatunnel /usr/local/bin/seatunnel
COPY --from=builder /build/config /opt/seatunnel/config

RUN mkdir -p /opt/seatunnel/data /opt/seatunnel/logs \
    && chown -R seatunnel:seatunnel /opt/seatunnel

USER seatunnel

EXPOSE 5000

ENTRYPOINT ["seatunnel"]
CMD ["--help"]
