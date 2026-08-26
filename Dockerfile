# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ===== Builder Stage =====
FROM rust:1.85-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    pkg-config \
    cmake \
    clang \
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

RUN cargo build --release --bin seatunnel --bin seatunnel-engine-server

# ===== Runtime Stage =====
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r seatunnel && useradd -r -g seatunnel seatunnel

WORKDIR /opt/seatunnel

COPY --from=builder /build/target/release/seatunnel /usr/local/bin/seatunnel
COPY --from=builder /build/target/release/seatunnel-engine-server /usr/local/bin/seatunnel-engine-server
COPY --from=builder /build/config /opt/seatunnel/config
COPY examples /opt/seatunnel/examples

RUN mkdir -p /opt/seatunnel/data /opt/seatunnel/state \
    && chown -R seatunnel:seatunnel /opt/seatunnel

ENV SEATUNNEL_STATE_DIR=/opt/seatunnel/state

USER seatunnel

# Master gRPC (workers/CLI) and worker port.
EXPOSE 5800 5001

# Default: run a master. Override with:
#   docker run seatunnel-rs seatunnel-engine-server --role worker \
#     --master <master>:5800 --worker-id w1
ENTRYPOINT ["seatunnel-engine-server"]
CMD ["--role", "master", "--addr", "0.0.0.0:5800"]
