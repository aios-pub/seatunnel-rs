//! TiKV CDC client: manages EventFeedV2 bidirectional streams per region.
//!
//! Each `RegionCdcStream` owns one gRPC stream to a TiKV node's
//! `/cdcpb.ChangeData/EventFeedV2`. The client sends one subscribe
//! `ChangeDataRequest` (the `request` oneof left unset, exactly like
//! official TiCDC) and receives a stream of `ChangeDataEvent`s containing
//! row changes and resolved-ts progress.
//!
//! Two lifecycle details matter against TiKV:
//! - Span keys must arrive pre-encoded in the memcomparable form (see
//!   `decoder::encode_comparable`) — the key space PD region boundaries and
//!   TiKV observed ranges use.
//! - The send side must stay open while subscribed (TiKV treats client EOF
//!   as a disconnect), but it must terminate when the handle is dropped —
//!   an immortal request stream leaves zombie server-side connections that
//!   poison TiKV's per-region CDC delegates. A oneshot guard ends the
//!   request stream (client half-close) on drop.

use std::time::Duration;

use futures::StreamExt;

use tonic::transport::Channel;
use tonic::Streaming;

use crate::kvproto::cdcpb::change_data_client::ChangeDataClient;
use crate::kvproto::cdcpb::change_data_request::KvApi;
use crate::kvproto::cdcpb::{ChangeDataEvent, ChangeDataRequest, Header};
use crate::kvproto::kvrpcpb::ExtraOp;
use crate::kvproto::metapb::RegionEpoch;

/// TiKV CDC service version advertised in the request header.
const TICDC_VERSION: &str = "8.1.0";

/// Events buffered per region before backpressure kicks in (the reader task
/// stops draining the gRPC stream and h2 flow control stalls the server).
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// One forwarded stream item: an event, or a terminal error message.
type StreamItem = Result<ChangeDataEvent, String>;

/// A single EventFeedV2 stream for one region.
pub struct RegionCdcStream {
    pub region_id: u64,
    pub request_id: u64,
    /// Events forwarded by the dedicated reader task that owns the tonic
    /// `Streaming`. Decouples polling (which may time out freely) from the
    /// actual stream future.
    events: tokio::sync::mpsc::Receiver<StreamItem>,
    /// Ends the request stream (half-close) on `close()`; dropping it has
    /// the same effect, making TiKV deregister the connection instead of
    /// leaking a zombie downstream.
    end_request: Option<tokio::sync::oneshot::Sender<()>>,
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
            // Byte-for-byte parity with official TiCDC: the `request` oneof
            // stays unset for a subscribe (TiKV treats `None` and `Register`
            // identically, but the official client never sets it).
            request: None,
            ..Default::default()
        }
    }

    /// Create a stream handle and immediately open the EventFeedV2 stream.
    #[allow(clippy::too_many_arguments)] // mirrors the TiKV CDC protocol handshake
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
        // Span keys are already memcomparable-encoded by the caller (see
        // `decoder::encode_comparable`) — send them verbatim.

        // EventFeedV2 is a bidi stream. The registration request goes first,
        // then the send side must STAY OPEN — closing it makes TiKV cancel
        // the whole stream (the server treats client EOF as disconnect).
        // The oneshot guard turns "handle dropped" into a graceful
        // half-close so TiKV deregisters the downstream promptly.
        //
        // The `features: stream-multiplexing` h2 header mirrors official
        // TiCDC: with it, TiKV buckets per-request state and tags ResolvedTs
        // messages with their request_id.
        let (end_tx, end_rx) = tokio::sync::oneshot::channel::<()>();
        let req_stream = futures::stream::iter(vec![request.clone()])
            .chain(futures::stream::pending::<ChangeDataRequest>())
            .take_until(async move {
                let _ = end_rx.await;
            });
        let mut grpc_req = tonic::Request::new(req_stream);
        grpc_req.metadata_mut().insert(
            "features",
            "stream-multiplexing".parse().expect("valid header value"),
        );
        let mut client = ChangeDataClient::new(channel);
        let response = client
            .event_feed_v2(grpc_req)
            .await
            .map_err(|e| anyhow::anyhow!("EventFeedV2 for region {} failed: {}", region_id, e))?;
        tracing::info!(
            "TiKV CDC: EventFeedV2 stream opened for region {}",
            region_id
        );

        // Dedicated reader task owns the tonic stream; the engine can poll
        // (and time out) freely without ever cancelling the stream future
        // mid-read.
        let (tx, rx) = tokio::sync::mpsc::channel(EVENT_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut stream: Streaming<ChangeDataEvent> = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(event)) => {
                        if tx.send(Ok(event)).await.is_err() {
                            break; // consumer gone
                        }
                    }
                    Ok(None) => {
                        let _ = tx.send(Err("stream closed by server".to_string())).await;
                        break;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(format!("stream error: {e}"))).await;
                        break;
                    }
                }
            }
            // Dropping `stream` here releases the RPC receive side.
        });

        Ok(RegionCdcStream {
            region_id,
            request_id,
            events: rx,
            end_request: Some(end_tx),
            base_request: request,
        })
    }

    /// Poll the next event batch. Returns `Ok(None)` when the stream is
    /// exhausted or closed; `Err` on a terminal stream failure.
    pub async fn next_event(&mut self) -> anyhow::Result<Option<ChangeDataEvent>> {
        tracing::trace!("TiKV CDC: next_event awaiting region {}", self.region_id);
        match self.events.recv().await {
            Some(Ok(event)) => Ok(Some(event)),
            Some(Err(msg)) => {
                tracing::warn!(
                    "TiKV CDC: stream ended for region {}: {}",
                    self.region_id,
                    msg
                );
                Err(anyhow::anyhow!(
                    "EventFeedV2 region {}: {}",
                    self.region_id,
                    msg
                ))
            }
            None => {
                tracing::warn!("TiKV CDC: reader task stopped for region {}", self.region_id);
                Ok(None)
            }
        }
    }

    /// Close the stream. Dropping the handle has the same effect: the
    /// request stream ends (half-close) and the reader task stops, making
    /// TiKV deregister the downstream.
    pub fn close(&mut self) {
        if let Some(end) = self.end_request.take() {
            let _ = end.send(());
        }
        self.events.close();
    }
}

/// A CDC client that can open streams to multiple TiKV nodes.
///
/// `connect_to_store` resolves a TiKV store address (host:port) into a
/// gRPC channel and opens an EventFeedV2 stream for a region.
pub struct CdcClient {
    /// Cluster id for request headers.
    cluster_id: u64,
    /// Base for per-stream request ids. TiKV keys subscriptions by
    /// `(region_id, request_id)` — a duplicate id from another client would
    /// silently REPLACE this subscription, so ids must be unique.
    request_id_base: u64,
}

impl CdcClient {
    pub fn new(cluster_id: u64) -> Self {
        // Derive a per-process random-ish base so concurrent clients never
        // collide on the same (region, request_id) subscription.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
            .unwrap_or(0);
        let base = (nanos << 12) ^ (std::process::id() as u64) | 1;
        CdcClient {
            cluster_id,
            request_id_base: base,
        }
    }

    fn next_request_id(&self) -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        self.request_id_base
            .wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Open an EventFeedV2 stream for one region on `tikv_addr` (host:port).
    #[allow(clippy::too_many_arguments)] // mirrors the TiKV CDC protocol handshake
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
            // Match grpc-go defaults from official TiCDC (grpc_conn.go):
            // InitialWindowSize = 65535, InitialConnWindowSize = 8MB
            .initial_stream_window_size(65_535)
            .initial_connection_window_size(8 * 1024 * 1024)
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to TiKV {}: {}", tikv_addr, e))?;

        let request_id = self.next_request_id();
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
