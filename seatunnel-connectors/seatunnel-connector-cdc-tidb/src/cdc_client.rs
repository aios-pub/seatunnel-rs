//! TiKV CDC client: manages EventFeedV2 bidirectional streams per region.
//!
//! Each `RegionCdcStream` owns one gRPC stream to a TiKV node's
//! `/cdcpb.ChangeData/EventFeedV2`. The client sends a `ChangeDataRequest`
//! (with a `Register`) and receives a stream of `ChangeDataEvent`s containing
//! row changes and resolved-ts progress.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Streaming;

use crate::kvproto::cdcpb::change_data_client::ChangeDataClient;
use crate::kvproto::cdcpb::change_data_request::{KvApi, Request};
use crate::kvproto::cdcpb::{ChangeDataEvent, ChangeDataRequest, Header};
use crate::kvproto::kvrpcpb::ExtraOp;
use crate::kvproto::metapb::RegionEpoch;

/// TiKV CDC service version advertised in the request header.
const TICDC_VERSION: &str = "8.0.0";

/// A single EventFeedV2 stream for one region.
pub struct RegionCdcStream {
    pub region_id: u64,
    pub request_id: u64,
    /// Stream of events from TiKV. Mutex so poll and retry can share it.
    stream: Arc<Mutex<Option<Streaming<ChangeDataEvent>>>>,
    /// The client used to (re)establish the stream on retry.
    client: ChangeDataClient<Channel>,
    /// Immutable request template (region_id, epoch, keys, checkpoint).
    pub base_request: ChangeDataRequest,
}

impl RegionCdcStream {
    /// Build the EventFeedV2 ChangeDataRequest template for a region.
    pub fn build_request(
        region_id: u64,
        epoch: Option<RegionEpoch>,
        start_key: &[u8],
        end_key: &[u8],
        checkpoint_ts: u64,
        request_id: u64,
        cluster_id: u64,
    ) -> ChangeDataRequest {
        ChangeDataRequest {
            header: Some(Header {
                cluster_id,
                ticdc_version: TICDC_VERSION.to_string(),
            }),
            region_id,
            region_epoch: epoch,
            checkpoint_ts,
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            request_id,
            extra_op: ExtraOp::ReadOldValue as i32,
            kv_api: KvApi::TiDb as i32,
            filter_loop: true,
            request: Some(Request::Register(Default::default())),
        }
    }

    /// Create a stream handle and immediately open the EventFeedV2 stream.
    pub async fn connect(
        channel: Channel,
        region_id: u64,
        epoch: Option<RegionEpoch>,
        start_key: &[u8],
        end_key: &[u8],
        checkpoint_ts: u64,
        request_id: u64,
        cluster_id: u64,
    ) -> anyhow::Result<Self> {
        let request = Self::build_request(
            region_id,
            epoch,
            start_key,
            end_key,
            checkpoint_ts,
            request_id,
            cluster_id,
        );
        // EventFeedV2 is a bidi stream: wrap the single ChangeDataRequest
        // into a one-shot stream that satisfies IntoStreamingRequest<Message = ChangeDataRequest>.
        use futures::stream;
        let req_stream = stream::iter(vec![request.clone()]);
        let mut client = ChangeDataClient::new(channel);
        let response = client
            .event_feed_v2(req_stream)
            .await
            .map_err(|e| anyhow::anyhow!("EventFeedV2 for region {} failed: {}", region_id, e))?;
        tracing::info!("TiKV CDC: EventFeedV2 stream opened for region {}", region_id);
        Ok(RegionCdcStream {
            region_id,
            request_id,
            stream: Arc::new(Mutex::new(Some(response.into_inner()))),
            client,
            base_request: request,
        })
    }

    /// Poll the stream for a batch of events.
    ///
    /// Returns `None` when the stream is exhausted or closed.
    pub async fn next_event(&self) -> anyhow::Result<Option<ChangeDataEvent>> {
        let mut guard = self.stream.lock().await;
        let stream = match guard.as_mut() {
            Some(s) => s,
            None => return Ok(None),
        };
        // `message()` yields individual framed messages.
        match stream.message().await {
            Ok(Some(event)) => Ok(Some(event)),
            Ok(None) => {
                tracing::warn!("TiKV CDC: stream closed for region {}", self.region_id);
                *guard = None;
                Ok(None)
            }
            Err(e) => {
                tracing::warn!(
                    "TiKV CDC: stream error for region {}: {}",
                    self.region_id,
                    e
                );
                *guard = None;
                Err(anyhow::anyhow!("EventFeedV2 stream error: {}", e))
            }
        }
    }

    /// Re-establish the stream using the stored request (used on retry).
    pub async fn reconnect(&mut self) -> anyhow::Result<()> {
        use futures::stream;
        let req_stream = stream::iter(vec![self.base_request.clone()]);
        let response = self
            .client
            .event_feed_v2(req_stream)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "EventFeedV2 reconnect for region {} failed: {}",
                    self.region_id,
                    e
                )
            })?;
        let mut guard = self.stream.lock().await;
        *guard = Some(response.into_inner());
        tracing::info!("TiKV CDC: reconnected region {}", self.region_id);
        Ok(())
    }

    /// Close the stream (deregister). Dropping the inner `Streaming`
    /// cancels the gRPC stream.
    pub async fn close(&self) {
        let mut guard = self.stream.lock().await;
        *guard = None;
    }
}

/// A CDC client that can open streams to multiple TiKV nodes.
///
/// `connect_to_store` resolves a TiKV store address (host:port) into a
/// gRPC channel and opens an EventFeedV2 stream for a region.
pub struct CdcClient {
    /// Cluster id for request headers.
    cluster_id: u64,
}

impl CdcClient {
    pub fn new(cluster_id: u64) -> Self {
        CdcClient { cluster_id }
    }

    /// Open an EventFeedV2 stream for one region on `tikv_addr` (host:port).
    pub async fn open_region_stream(
        &self,
        tikv_addr: &str,
        region_id: u64,
        epoch: Option<RegionEpoch>,
        start_key: &[u8],
        end_key: &[u8],
        checkpoint_ts: u64,
    ) -> anyhow::Result<RegionCdcStream> {
        let addr = if tikv_addr.starts_with("http://") {
            tikv_addr.to_string()
        } else {
            format!("http://{}", tikv_addr)
        };
        let channel = Channel::from_shared(addr)
            .map_err(|e| anyhow::anyhow!("Invalid TiKV address: {}", e))?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to TiKV {}: {}", tikv_addr, e))?;

        let request_id = region_id.wrapping_mul(1000).wrapping_add(1);
        RegionCdcStream::connect(
            channel,
            region_id,
            epoch,
            start_key,
            end_key,
            checkpoint_ts,
            request_id,
            self.cluster_id,
        )
        .await
    }
}