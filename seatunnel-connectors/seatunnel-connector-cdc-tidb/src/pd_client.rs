//! PD (Placement Driver) client: discovers and watches TiKV regions.
//!
//! Responsibilities:
//! - Connect to PD via gRPC
//! - `get_region(key)`: find the region containing a key
//! - `get_region_by_id(id)`: fetch region metadata by id (split/merge)
//! - `scan_regions(start, end)`: enumerate all regions in a key range

use std::time::Duration;

use tonic::transport::Channel;

use crate::kvproto::metapb::Region;
use crate::kvproto::pdpb::pd_client::PdClient as PdGrpcClient;
use crate::kvproto::pdpb::{
    GetRegionRequest, GetRegionResponse, RequestHeader, ScanRegionsRequest,
};

/// Cluster id placeholder used in PD request headers.
const DEFAULT_CLUSTER_ID: u64 = 0;

/// A minimal PD client for region discovery.
pub struct PdClient {
    inner: PdGrpcClient<Channel>,
    /// Cluster id learned from PD's GetMembers header — every subsequent
    /// request must echo it or PD rejects the call.
    cluster_id: u64,
}

/// Result of a region lookup.
#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub region: Region,
    /// Leader peer address (host:port) if known.
    pub leader_addr: Option<String>,
}

impl PdClient {
    /// Connect to a PD endpoint (e.g. "127.0.0.1:2379" or
    /// "http://127.0.0.1:2379") and resolve the cluster id via GetMembers.
    pub async fn connect(pd_addr: &str) -> anyhow::Result<Self> {
        // tonic requires an explicit scheme; accept bare host:port inputs.
        let uri = if pd_addr.starts_with("http://") || pd_addr.starts_with("https://") {
            pd_addr.to_string()
        } else {
            format!("http://{}", pd_addr)
        };
        let channel = Channel::from_shared(uri.clone())
            .map_err(|e| anyhow::anyhow!("Invalid PD address {}: {}", uri, e))?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PD {}: {}", uri, e))?;
        let mut inner = PdGrpcClient::new(channel);

        // Bootstrap the cluster id from GetMembers; the response header
        // carries the authoritative value.
        use crate::kvproto::pdpb::GetMembersRequest;
        let members_req = GetMembersRequest {
            header: Some(RequestHeader {
                cluster_id: DEFAULT_CLUSTER_ID,
                ..Default::default()
            }),
        };
        let members_resp = inner
            .get_members(tonic::Request::new(members_req))
            .await
            .map_err(|e| anyhow::anyhow!("PD get_members failed: {}", e))?
            .into_inner();
        let cluster_id = members_resp
            .header
            .as_ref()
            .map(|h| h.cluster_id)
            .unwrap_or(DEFAULT_CLUSTER_ID);
        if cluster_id == DEFAULT_CLUSTER_ID {
            anyhow::bail!("PD at {} did not report a cluster id", pd_addr);
        }
        tracing::info!(
            "TiDB CDC: connected to PD {} (cluster_id={})",
            pd_addr,
            cluster_id
        );

        Ok(PdClient { inner, cluster_id })
    }

    fn header(&self) -> Option<RequestHeader> {
        Some(RequestHeader {
            cluster_id: self.cluster_id,
            ..Default::default()
        })
    }

    /// The cluster id resolved at connect time.
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Request one timestamp oracle tick from PD. This is the canonical
    /// "start from now" position for a TiKV change stream (checkpoint_ts=0
    /// is rejected by TiKV's CDC service).
    pub async fn get_tso(&mut self) -> anyhow::Result<u64> {
        use crate::kvproto::pdpb::TsoRequest;
        let req = TsoRequest {
            header: self.header(),
            count: 1,
            ..Default::default()
        };
        // Tso is a client-streaming RPC.
        use futures::stream;
        let req_stream = stream::iter(vec![req]);
        let mut stream = self
            .inner
            .tso(tonic::Request::new(req_stream))
            .await
            .map_err(|e| anyhow::anyhow!("PD tso request failed: {}", e))?
            .into_inner();
        let resp = stream
            .message()
            .await
            .map_err(|e| anyhow::anyhow!("PD tso stream failed: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("PD tso stream closed without a response"))?;
        let ts = resp
            .timestamp
            .ok_or_else(|| anyhow::anyhow!("PD tso response missing timestamp"))?;
        // Compose the TiDB timestamp oracle: physical ms << 18 | logical.
        let logical_mask: u64 = (1 << 18) - 1;
        Ok(((ts.physical.max(0) as u64) << 18) | ((ts.logical as u64) & logical_mask))
    }

    /// Find the region that contains `key`.
    pub async fn get_region(&mut self, key: &[u8]) -> anyhow::Result<Option<RegionInfo>> {
        let req = GetRegionRequest {
            header: self.header(),
            region_key: key.to_vec(),
            need_buckets: false,
        };
        let resp: GetRegionResponse = self
            .inner
            .get_region(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("PD get_region failed: {}", e))?
            .into_inner();
        Ok(self.map_region_response(resp))
    }

    /// Get region metadata by region id.
    pub async fn get_region_by_id(&mut self, region_id: u64) -> anyhow::Result<Option<RegionInfo>> {
        use crate::kvproto::pdpb::GetRegionByIdRequest;
        let req = GetRegionByIdRequest {
            header: self.header(),
            region_id,
            need_buckets: false,
        };
        let resp = self
            .inner
            .get_region_by_id(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("PD get_region_by_id failed: {}", e))?
            .into_inner();
        Ok(self.map_region_response(resp))
    }

    /// Enumerate all regions covering [start_key, end_key).
    pub async fn scan_regions(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> anyhow::Result<Vec<RegionInfo>> {
        use crate::kvproto::pdpb::ScanRegionsResponse;
        let req = ScanRegionsRequest {
            header: self.header(),
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            limit: 0,
        };
        let resp: ScanRegionsResponse = self
            .inner
            .scan_regions(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("PD scan_regions failed: {}", e))?
            .into_inner();
        let mut out: Vec<RegionInfo> = Vec::new();
        // region_metas carries the metapb::Region list (backward-compatible field).
        for region in resp.region_metas {
            out.push(RegionInfo {
                region,
                leader_addr: None,
            });
        }
        // If empty, fall back to per-key probing.
        if out.is_empty() && !start_key.is_empty() {
            if let Some(ri) = self.get_region(start_key).await? {
                out.push(ri);
            }
        }
        Ok(out)
    }

    fn map_region_response(&self, resp: GetRegionResponse) -> Option<RegionInfo> {
        let region = resp.region?;
        // Keep leader metadata; full store address resolution is done via
        // get_store_address(store_id) when the leader is known.
        let _ = resp.leader;
        Some(RegionInfo {
            region,
            leader_addr: None,
        })
    }

    /// Resolve a TiKV store address (host:port) by store id via PD GetStore.
    pub async fn get_store_address(&mut self, store_id: u64) -> anyhow::Result<Option<String>> {
        use crate::kvproto::pdpb::GetStoreRequest;
        let req = GetStoreRequest {
            header: self.header(),
            store_id,
        };
        let resp = self
            .inner
            .get_store(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("PD get_store failed: {}", e))?
            .into_inner();
        Ok(resp.store.and_then(|s| {
            if s.address.is_empty() {
                None
            } else {
                Some(s.address)
            }
        }))
    }

    /// Look up the leader store address for a region.
    pub async fn leader_address(&mut self, ri: &RegionInfo) -> anyhow::Result<Option<String>> {
        // Find the leader peer (role == 0 is Voter) and resolve its store.
        let leader_peer = ri
            .region
            .peers
            .iter()
            .find(|p| p.role == 0) // Voter
            .or_else(|| ri.region.peers.first());
        match leader_peer {
            Some(peer) => self.get_store_address(peer.store_id).await,
            None => Ok(None),
        }
    }
}
