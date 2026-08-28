/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Raft transport over the existing tonic gRPC stack.
//!
//! Outbound: `GrpcNetwork(Factory)` implements openraft's network traits
//! by serializing the typed RPCs into the RaftService byte payloads.
//! Inbound: `RaftServiceHandler` feeds decoded messages into the local
//! `Raft` instance.
//!
//! Error mapping is intentionally simple: transport failures become
//! `Unreachable` (openraft retries with backoff); logical rejections
//! ride inside the response payloads. Snapshots are small JSON and are
//! sent in ONE InstallSnapshot call, so no chunk assembly is needed.

use std::collections::BTreeMap;

use openraft::error::{Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, Snapshot};

use seatunnel_engine_comm::generated::raft_service_client::RaftServiceClient;
use seatunnel_engine_comm::generated::raft_service_server::RaftService;
use seatunnel_engine_comm::generated::{
    RaftAppendRequest, RaftAppendResponse, RaftSnapshotRequest, RaftSnapshotResponse,
    RaftVoteRequest, RaftVoteResponse,
};
use tonic::{Request, Response, Status};

use super::Types;

/// Creates per-target connections; the member map is static.
pub struct GrpcNetworkFactory {
    pub members: BTreeMap<u64, BasicNode>,
}

pub struct GrpcNetwork {
    target: u64,
    addr: String,
}

fn unreachable(
    target: u64,
    addr: &str,
    e: impl std::fmt::Display,
) -> RPCError<u64, BasicNode, RaftError<u64>> {
    tracing::debug!("raft rpc to {} ({}) failed: {}", target, addr, e);
    let err = std::io::Error::other(format!("node {} ({}): {}", target, addr, e));
    RPCError::Unreachable(Unreachable::new(&err))
}

#[allow(refining_impl_trait)]
impl RaftNetworkFactory<Types> for GrpcNetworkFactory {
    type Network = GrpcNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        GrpcNetwork {
            target,
            addr: node.addr.clone(),
        }
    }
}

/// Serialize/deserialize helpers (JSON keeps the wire debuggable); call
/// sites map the string error into their own RPCError type.
fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|e| e.to_string())
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}
fn net_err<E: std::fmt::Display>(e: E) -> NetworkError {
    NetworkError::new(&std::io::Error::other(e.to_string()))
}

impl GrpcNetwork {
    async fn client(&self) -> Result<RaftServiceClient<tonic::transport::Channel>, RPCError<u64, BasicNode, RaftError<u64>>> {
        RaftServiceClient::connect(format!("http://{}", self.addr))
            .await
            .map_err(|e| unreachable(self.target, &self.addr, e))
    }
}

impl RaftNetwork<Types> for GrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Types>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64>>,
    > {
        let mut client = self.client().await?;
        let req = Request::new(RaftAppendRequest {
            payload: encode(&rpc).map_err(|e| RPCError::Network(net_err(e)))?,
        });
        let resp = client
            .append_entries(req)
            .await
            .map_err(|e| unreachable(self.target, &self.addr, e))?;
        decode(&resp.into_inner().payload).map_err(|e| RPCError::Network(net_err(e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<Types>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let mut client = self
            .client()
            .await
            .map_err(|e| map_snapshot_rpc(e))?;
        let req = Request::new(RaftSnapshotRequest {
            payload: encode(&rpc).map_err(|e| map_snapshot_rpc(RPCError::Network(net_err(e))))?,
        });
        let resp = client
            .install_snapshot(req)
            .await
            .map_err(|e| map_snapshot_rpc(unreachable(self.target, &self.addr, e)))?;
        decode(&resp.into_inner().payload).map_err(|e| {
            map_snapshot_rpc(RPCError::Network(net_err(e)))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client().await?;
        let req = Request::new(RaftVoteRequest {
            payload: encode(&rpc).map_err(|e| RPCError::Network(net_err(e)))?,
        });
        let resp = client
            .vote(req)
            .await
            .map_err(|e| unreachable(self.target, &self.addr, e))?;
        decode(&resp.into_inner().payload).map_err(|e| RPCError::Network(net_err(e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<u64>,
        snapshot: Snapshot<Types>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<openraft::raft::SnapshotResponse<u64>, StreamingError<Types, Fatal<u64>>> {
        // Snapshots here are small JSON documents — one-shot transfer.
        use tokio::io::AsyncReadExt;
        let mut data = *snapshot.snapshot;
        let mut bytes = Vec::new();
        if let Err(e) = data.read_to_end(&mut bytes).await {
            return Err(StreamingError::Network(net_err(format!(
                "read local snapshot: {}",
                e
            ))));
        }
        let meta = snapshot.meta;
        let rpc = InstallSnapshotRequest::<Types> {
            vote,
            offset: 0,
            data: bytes,
            done: true,
            meta,
        };
        let mut client = self.client().await.map_err(|e| streaming_from_rpc(e))?;
        let req = Request::new(RaftSnapshotRequest {
            payload: encode(&rpc)
                .map_err(|e| streaming_from_rpc(RPCError::Network(net_err(e))))?,
        });
        let resp = client
            .install_snapshot(req)
            .await
            .map_err(|e| streaming_from_rpc(unreachable(self.target, &self.addr, e)))?;
        let decoded: openraft::raft::InstallSnapshotResponse<u64> = decode(
            &resp.into_inner().payload,
        )
        .map_err(|e| streaming_from_rpc(RPCError::Network(net_err(e))))?;
        Ok(openraft::raft::SnapshotResponse {
            vote: decoded.vote,
        })
    }
}

fn map_snapshot_rpc(
    e: RPCError<u64, BasicNode, RaftError<u64>>,
) -> RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>> {
    match e {
        RPCError::Unreachable(u) => RPCError::Unreachable(u),
        RPCError::Timeout(t) => RPCError::Timeout(t),
        RPCError::Network(n) => RPCError::Network(n),
        RPCError::PayloadTooLarge(_) | RPCError::RemoteError(_) => {
            RPCError::Network(net_err("snapshot rpc logical error mapped to network"))
        }
    }
}

fn streaming_from_rpc(
    e: RPCError<u64, BasicNode, RaftError<u64>>,
) -> StreamingError<Types, Fatal<u64>> {
    // Snapshot transport failures are transient network issues from the
    // caller's perspective — map everything onto Network/Unreachable.
    match e {
        RPCError::Unreachable(u) => StreamingError::Unreachable(u),
        RPCError::Timeout(t) => StreamingError::Timeout(t),
        RPCError::Network(n) => StreamingError::Network(n),
        other => StreamingError::Network(net_err(other)),
    }
}

/// Inbound side: decode payloads and feed the local raft instance.
pub struct RaftServiceHandler {
    pub raft: super::Raft,
}

fn bad_payload(e: impl std::fmt::Display) -> Status {
    Status::invalid_argument(format!("raft payload decode error: {}", e))
}

#[tonic::async_trait]
impl RaftService for RaftServiceHandler {
    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        let rpc: VoteRequest<u64> =
            serde_json::from_slice(&request.into_inner().payload).map_err(bad_payload)?;
        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::unavailable(format!("vote: {}", e)))?;
        let payload = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftVoteResponse { payload }))
    }

    async fn append_entries(
        &self,
        request: Request<RaftAppendRequest>,
    ) -> Result<Response<RaftAppendResponse>, Status> {
        let rpc: AppendEntriesRequest<Types> =
            serde_json::from_slice(&request.into_inner().payload).map_err(bad_payload)?;
        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::unavailable(format!("append_entries: {}", e)))?;
        let payload = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftAppendResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftSnapshotRequest>,
    ) -> Result<Response<RaftSnapshotResponse>, Status> {
        let rpc: InstallSnapshotRequest<Types> =
            serde_json::from_slice(&request.into_inner().payload).map_err(bad_payload)?;
        // One-shot transfer (done && offset == 0), asserted by the sender.
        if !rpc.done || rpc.offset != 0 {
            return Err(Status::unimplemented(
                "chunked snapshot transfer not supported; snapshots are one-shot JSON",
            ));
        }
        let (vote, meta, data) = (rpc.vote, rpc.meta, rpc.data);
        let snapshot = openraft::Snapshot::<Types> {
            meta,
            snapshot: Box::new(std::io::Cursor::new(data)),
        };
        self.raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::unavailable(format!("install_full_snapshot: {}", e)))?;
        let resp = openraft::raft::InstallSnapshotResponse { vote };
        let payload = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftSnapshotResponse { payload }))
    }
}
