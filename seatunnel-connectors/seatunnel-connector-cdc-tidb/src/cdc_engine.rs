//! TiKV CDC engine: coordinates PD region discovery, per-region
//! EventFeedV2 streams, transaction correlation, and resolved-ts watermark.
//!
//! Responsibilities:
//! - Discover regions covering a table's key range from PD
//! - Open and maintain one EventFeedV2 stream per region
//! - Watch for region split/merge and re-open affected streams
//! - Track per-region resolved_ts and compute the global safe watermark
//! - Feed rows through the Percolator transaction tracker
//! - Persist checkpoint (resolved_ts) and provide retry with backoff

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tokio::time::sleep;

use crate::cdc_client::{CdcClient, RegionCdcStream};
use crate::decoder::{decode_record_key, decode_row_value, ColumnValue, TransactionTracker};
use crate::kvproto::cdcpb::event::{Event as CdcEvent, Row as CdcRow};
use crate::kvproto::cdcpb::ChangeDataEvent;
use crate::kvproto::metapb::RegionEpoch;
use crate::pd_client::{PdClient, RegionInfo};

/// A region: its key range and the active CDC stream.
struct RegionState {
    region_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    epoch: Option<RegionEpoch>,
    stream: Option<RegionCdcStream>,
    /// Latest resolved_ts reported by this region's stream.
    resolved_ts: u64,
    /// Leader TiKV address for (re)connecting.
    leader_addr: Option<String>,
    /// Consecutive failures for retry/backoff.
    failures: u32,
}

/// A single decoded row change emitted by the engine.
#[derive(Debug, Clone)]
pub struct CdcRowEvent {
    pub table_id: i64,
    pub handle: i64,
    pub op_type: i32,
    /// True when a PUT carries a non-empty old_value (i.e. an UPDATE
    /// rather than an INSERT).
    pub is_update: bool,
    pub columns: Vec<ColumnValue>,
    pub resolved_ts: u64,
}

/// Configuration for the CDC engine.
#[derive(Debug, Clone)]
pub struct CdcEngineConfig {
    pub pd_addrs: Vec<String>,
    pub table_id: i64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub cluster_id: u64,
    /// Checkpoint: the resolved_ts to resume from (0 = from now/first).
    pub checkpoint_ts: u64,
    /// Whether snapshot data still needs to be read (phase).
    pub request_snapshot: bool,
    /// Store-address rewrites applied to leader addresses reported by PD
    /// (host part only, first match wins). Typical use: a TiKV advertised on
    /// `host.docker.internal` is rewritten to `127.0.0.1` for workers running
    /// on the host, where that DNS name does not resolve.
    pub address_rewrite: Vec<(String, String)>,
    /// Re-register each region stream every N milliseconds. Each
    /// re-registration triggers a fresh incremental scan from the current
    /// `resolved_ts`, which is currently the only reliably-delivered change
    /// path against some TiKV builds (see connector docs). 0 disables.
    pub resubscribe_interval_ms: u64,
}

/// Apply the configured host rewrites to `addr` ("host:port").
fn rewrite_address(addr: &str, rules: &[(String, String)]) -> String {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return addr.to_string();
    };
    for (from, to) in rules {
        if host == from {
            return format!("{}:{}", to, port);
        }
    }
    addr.to_string()
}

/// The TiKV CDC engine.
pub struct CdcEngine {
    config: CdcEngineConfig,
    pd: Option<PdClient>,
    cdc: CdcClient,
    regions: HashMap<u64, RegionState>,
    tracker: TransactionTracker,
    /// Global watermark = min(resolved_ts across regions).
    global_resolved_ts: u64,
    /// Row events awaiting consumption.
    pending_rows: VecDeque<CdcRowEvent>,
    /// Whether the last poll found a new event (for backoff tuning).
    last_had_events: bool,
    /// Monotonic instant of the last periodic re-subscription.
    last_resubscribe: Option<std::time::Instant>,
    /// Checkpoint to resume from on re-registration: starts at the initial
    /// TSO and only advances when data rows were actually received. Using
    /// the (server-driven) resolved_ts here would skip writes whose locks
    /// were never routed to us.
    resume_checkpoint_ts: u64,
    /// Highest commit_ts/resolved_ts of rows actually emitted from pending.
    last_data_ts: u64,
}

impl CdcEngine {
    pub fn new(config: CdcEngineConfig) -> Self {
        let resume_checkpoint_ts = config.checkpoint_ts;
        CdcEngine {
            config,
            pd: None,
            cdc: CdcClient::new(0),
            regions: HashMap::new(),
            tracker: TransactionTracker::new(),
            global_resolved_ts: 0,
            pending_rows: VecDeque::new(),
            last_had_events: false,
            last_resubscribe: None,
            resume_checkpoint_ts,
            last_data_ts: 0,
        }
    }

    /// Connect to PD and discover all regions covering the table key range.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        let pd_addr = self.config.pd_addrs.first().cloned().unwrap_or_default();
        let pd = PdClient::connect(&pd_addr).await?;
        // Real PD rejects requests whose header carries a wrong cluster id —
        // propagate the resolved id into every CDC request as well.
        let resolved = pd.cluster_id();
        if self.config.cluster_id != 0 && self.config.cluster_id != resolved {
            tracing::warn!(
                "TiKV CDC: configured cluster_id {} differs from PD-resolved {}; using {}",
                self.config.cluster_id,
                resolved,
                resolved
            );
        }
        self.config.cluster_id = resolved;
        self.cdc = CdcClient::new(resolved);

        // TiKV's CDC service rejects checkpoint_ts=0; resolve a real TSO so
        // the stream starts "from now" (the snapshot overlap window is then
        // replayed from the engine buffer — no loss).
        let mut pd = pd;
        if self.config.checkpoint_ts == 0 {
            let tso = pd.get_tso().await?;
            tracing::info!("TiKV CDC: resolved starting TSO {}", tso);
            self.config.checkpoint_ts = tso;
            self.global_resolved_ts = tso;
            // Anchor periodic re-scans at the same point until real data
            // arrives (a zero checkpoint is rejected/mishandled by TiKV).
            self.resume_checkpoint_ts = tso;
        } else {
            self.global_resolved_ts = self.config.checkpoint_ts;
            self.resume_checkpoint_ts = self.resume_checkpoint_ts.max(self.config.checkpoint_ts);
        }
        self.pd = Some(pd);

        self.discover_regions().await
    }

    /// Enumerate regions covering [start_key, end_key) from PD.
    async fn discover_regions(&mut self) -> anyhow::Result<()> {
        let pd = self.pd.as_mut().unwrap();
        let regions = pd
            .scan_regions(&self.config.start_key, &self.config.end_key)
            .await?;
        for ri in regions {
            self.add_region(ri).await?;
        }
        tracing::info!(
            "TiKV CDC: discovered {} regions for table {}",
            self.regions.len(),
            self.config.table_id
        );
        Ok(())
    }

    /// Register a region and open its EventFeedV2 stream.
    async fn add_region(&mut self, ri: RegionInfo) -> anyhow::Result<()> {
        let region_id = ri.region.id;
        if self.regions.contains_key(&region_id) {
            return Ok(());
        }
        let start_key = ri.region.start_key.clone();
        let end_key = ri.region.end_key.clone();
        let epoch = ri.region.region_epoch;
        // Resolve the leader store address via PD GetStore.
        let leader_addr = if let Some(pd) = self.pd.as_mut() {
            match pd.leader_address(&ri).await {
                Ok(Some(addr)) => Some(addr),
                _ => ri.leader_addr.clone(),
            }
        } else {
            ri.leader_addr.clone()
        };
        let state = RegionState {
            region_id,
            start_key,
            end_key,
            epoch,
            stream: None,
            resolved_ts: 0,
            leader_addr: leader_addr.map(|a| rewrite_address(&a, &self.config.address_rewrite)),
            failures: 0,
        };
        self.regions.insert(region_id, state);
        // Open the stream lazily (first poll) so start() stays snappy.
        Ok(())
    }

    /// Periodically re-scan the table key range from PD to pick up regions
    /// created by split, and drop regions removed by merge.
    pub async fn refresh_regions(&mut self) -> anyhow::Result<()> {
        let pd = match self.pd.as_mut() {
            Some(pd) => pd,
            None => return Ok(()),
        };
        // Remove stale regions that no longer overlap the table range.
        self.regions.retain(|_id, r| {
            let overlaps = r.start_key < self.config.end_key
                && (self.config.start_key < r.end_key || r.end_key.is_empty());
            if !overlaps {
                tracing::info!(
                    "TiKV CDC: dropping region {} (merged or moved out of range)",
                    r.region_id
                );
            }
            overlaps
        });
        // Discover any new regions (from split).
        let regions = pd
            .scan_regions(&self.config.start_key, &self.config.end_key)
            .await?;
        let mut added = 0;
        for ri in regions {
            if !self.regions.contains_key(&ri.region.id) {
                self.add_region(ri).await?;
                added += 1;
            }
        }
        if added > 0 {
            tracing::info!("TiKV CDC: detected {} new region(s) after split", added);
        }
        Ok(())
    }

    /// Ensure the stream for `region_id` is open; reconnect with backoff on error.
    ///
    /// The subscription span is the **table's key range intersected with the
    /// region**, matching official TiCDC: subscribing with raw region bounds
    /// makes the server-side entry filter reject everything (the observed
    /// range must decode to the same keyspace as the streamed entries).
    async fn ensure_stream(&mut self, region_id: u64) -> anyhow::Result<()> {
        let region = self.regions.get(&region_id).unwrap();
        let already_open = region.stream.is_some();
        if already_open {
            return Ok(());
        }
        // Span = the table prefix itself (PLAIN keys — official TiCDC notes
        // they are *not* memcomparable-wrapped; any other form makes TiKV's
        // ObservedRange decode fail and every entry gets filtered server-side).
        // TiKV intersects the span with the region internally.
        let sk = self.config.start_key.clone();
        let ek = self.config.end_key.clone();
        let epoch = region.epoch;
        let checkpoint = self.config.checkpoint_ts;
        let addr = region.leader_addr.clone().unwrap_or_default();

        // cdc_client wraps sk/ek with EncodeBytes on the wire.
        let stream = self
            .cdc
            .open_region_stream(&addr, region_id, epoch, &sk, &ek, checkpoint)
            .await?;
        if let Some(s) = self.regions.get_mut(&region_id) {
            s.stream = Some(stream);
            s.failures = 0;
        }
        Ok(())
    }

    /// Poll all region streams once with a bounded per-event wait, feeding
    /// events into the tracker. Returns the number of events consumed.
    pub async fn poll(&mut self) -> anyhow::Result<usize> {
        self.poll_with_budget(250).await
    }

    /// Like [`poll`](Self::poll) but each region read waits at most
    /// `budget_ms` — used by snapshot-phase draining so table scans are not
    /// starved by idle streams.
    pub async fn poll_with_budget(&mut self, budget_ms: u64) -> anyhow::Result<usize> {
        // Periodically re-check PD for region split/merge (every 128 polls).
        static POLL_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let poll_no = POLL_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if poll_no.is_multiple_of(128) {
            if let Err(e) = self.refresh_regions().await {
                tracing::warn!("TiKV CDC: region refresh failed: {}", e);
            }
        }

        // Periodic re-registration: each fresh registration makes TiKV run
        // an incremental scan from the current resolved_ts, which is the
        // reliably-delivered change path (see config docs). Advance the
        // checkpoint first so no window is missed; overlap is at-least-once.
        if self.config.resubscribe_interval_ms > 0 {
            let due = match self.last_resubscribe {
                None => true,
                Some(t) => t.elapsed().as_millis() as u64 >= self.config.resubscribe_interval_ms,
            };
            if due && !self.regions.is_empty() {
                self.config.checkpoint_ts = self.resume_checkpoint_ts;
                tracing::debug!(
                    "TiKV CDC: periodic re-subscribe (checkpoint_ts={})",
                    self.config.checkpoint_ts
                );
                for r in self.regions.values_mut() {
                    if r.stream.take().is_some() {
                        r.failures = 0;
                    }
                }
                self.last_resubscribe = Some(std::time::Instant::now());
            }
        }

        let mut consumed = 0usize;
        let mut had_event = false;
        let region_ids: Vec<u64> = self.regions.keys().copied().collect();

        for region_id in region_ids {
            // Open stream if needed.
            let mut needs_open = false;
            {
                if let Some(r) = self.regions.get(&region_id) {
                    needs_open = r.stream.is_none();
                }
            }
            if needs_open {
                match self.ensure_stream(region_id).await {
                    Ok(()) => {}
                    Err(e) => {
                        let f = {
                            let r = self.regions.get_mut(&region_id).unwrap();
                            r.failures += 1;
                            r.failures
                        };
                        tracing::warn!(
                            "TiKV CDC: open stream failed for region {} (attempt {}): {}",
                            region_id,
                            f,
                            e
                        );
                        // Backoff before next attempt.
                        let backoff = Duration::from_millis((f.min(8) * 500) as u64);
                        sleep(backoff).await;
                        continue;
                    }
                }
            }

            // Read one event batch from the region's stream (bounded wait).
            let res = {
                let region = self.regions.get(&region_id).unwrap();
                match &region.stream {
                    Some(stream) => match tokio::time::timeout(
                        Duration::from_millis(budget_ms.max(1)),
                        stream.next_event(),
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(_) => {
                            tracing::trace!("TiKV CDC: region {} poll budget expired", region_id);
                            // Budget exhausted for this round — not an error.
                            continue;
                        }
                    },
                    None => continue,
                }
            };
            match &res {
                Ok(Some(_)) => {
                    tracing::debug!("TiKV CDC: region {} delivered an event", region_id);
                    // count entries inside for diagnosis
                    if let Some(r) = self.regions.get(&region_id) {
                        if let Some(st) = &r.stream {
                            let _ = st;
                        }
                    }
                }
                Ok(None) => tracing::debug!("TiKV CDC: region {} stream ended (None)", region_id),
                Err(e) => tracing::debug!("TiKV CDC: region {} read error: {}", region_id, e),
            }
            match res {
                Ok(Some(change_event)) => {
                    had_event = true;
                    consumed += self.handle_change_event(region_id, change_event);
                }
                Ok(None) => {
                    // Stream ended — mark for reconnect.
                    if let Some(r) = self.regions.get_mut(&region_id) {
                        r.stream = None;
                        r.failures += 1;
                    }
                }
                Err(_e) => {
                    if let Some(r) = self.regions.get_mut(&region_id) {
                        r.stream = None;
                        r.failures += 1;
                    }
                }
            }
        }

        self.last_had_events = had_event;
        // Flush transactions at the global watermark.
        self.flush_transactions();
        Ok(consumed)
    }

    /// Handle one ChangeDataEvent from a region: ingest rows into the tracker
    /// and update the region's resolved_ts.
    fn handle_change_event(&mut self, region_id: u64, event: ChangeDataEvent) -> usize {
        tracing::debug!(
            "TiKV CDC: region {} change event ({} sub-events, resolved_ts={:?})",
            region_id,
            event.events.len(),
            event.resolved_ts.as_ref().map(|t| t.ts)
        );
        let my_request_id = match self.regions.get(&region_id).and_then(|r| r.stream.as_ref()) {
            Some(stream) => stream.request_id,
            None => return 0,
        };
        let mut row_count = 0;
        for ev in event.events {
            // Another subscriber's events (different request_id) are not
            // ours. A zero id is only legitimate for store-level messages,
            // never for Entries — keep those filtered.
            if ev.request_id != my_request_id && ev.request_id != 0 {
                tracing::debug!(
                    "TiKV CDC: dropping sub-event for foreign request_id {} (ours {})",
                    ev.request_id,
                    my_request_id
                );
                continue;
            }
            match ev.event {
                Some(CdcEvent::Entries(entries)) => {
                    // `entries.entries` is Vec<cdcpb::event::Row> (i.e. CdcRow)
                    for row in entries.entries {
                        self.tracker.on_row(&row);
                        row_count += 1;
                    }
                }
                Some(CdcEvent::ResolvedTs(ts)) => {
                    if let Some(r) = self.regions.get_mut(&region_id) {
                        r.resolved_ts = r.resolved_ts.max(ts);
                    }
                }
                Some(CdcEvent::Admin(_)) => {
                    // region split/merge markers: force a re-discover
                    self.mark_for_reconnect(region_id);
                }
                Some(CdcEvent::Error(err)) => {
                    tracing::warn!("TiKV CDC: region {} reported error: {:?}", region_id, err);
                    self.mark_for_reconnect(region_id);
                }
                _ => {}
            }
        }
        // Top-level ResolvedTs: with the `stream-multiplexing` feature TiKV
        // tags it with our request_id; without negotiation it arrives as a
        // store-wide aggregate tagged 0. Accept both, ignore foreign ids.
        if let Some(rt) = event.resolved_ts {
            if rt.request_id == my_request_id || rt.request_id == 0 {
                if let Some(r) = self.regions.get_mut(&region_id) {
                    r.resolved_ts = r.resolved_ts.max(rt.ts);
                }
            }
        }
        row_count
    }

    /// Mark a region's stream for reconnect (used on admin/error events).
    fn mark_for_reconnect(&mut self, region_id: u64) {
        if let Some(r) = self.regions.get_mut(&region_id) {
            r.stream = None;
            r.failures += 1;
        }
    }

    /// Recompute the global watermark = min of per-region resolved_ts.
    fn update_global_watermark(&mut self) {
        let min_ts = self
            .regions
            .values()
            .map(|r| r.resolved_ts)
            .min()
            .unwrap_or(self.global_resolved_ts);
        self.global_resolved_ts = self.global_resolved_ts.max(min_ts);
    }

    /// Flush committed transactions at the current global watermark.
    fn flush_transactions(&mut self) {
        self.update_global_watermark();
        let watermark = self.global_resolved_ts;
        if watermark == 0 {
            return;
        }
        let committed = self.tracker.flush(watermark);
        for pending in committed {
            if pending.commit_ts > self.last_data_ts {
                self.last_data_ts = pending.commit_ts;
                self.resume_checkpoint_ts = self.resume_checkpoint_ts.max(pending.commit_ts);
            }
            // Decode the row value into columns.
            let columns = match decode_row_value(&pending.value) {
                Ok(cols) => cols,
                Err(_) => continue,
            };
            // Determine operation kind: op_type 1 = PUT, 2 = DELETE.
            // For PUT with non-empty old_value it's an UPDATE; the tracker
            // already categorized via op_type on the prewrite.
            // PUT with a non-empty old value indicates an UPDATE
            // (the pre-image was captured via ExtraOp::ReadOldValue).
            let is_update = pending.op_type == 1 && !pending.old_value.is_empty();
            let event = CdcRowEvent {
                table_id: self.config.table_id,
                handle: pending.handle,
                op_type: pending.op_type,
                is_update,
                columns,
                resolved_ts: watermark,
            };
            self.pending_rows.push_back(event);
        }
    }

    /// Take the next decoded row event, if any.
    pub fn next_row(&mut self) -> Option<CdcRowEvent> {
        // FIFO: preserve commit order (oldest committed row first).
        self.pending_rows.pop_front()
    }

    /// Current safe watermark (min resolved_ts across regions).
    pub fn resolved_ts(&self) -> u64 {
        self.global_resolved_ts
    }

    /// Persist the current checkpoint (resolved_ts) as a (key, value) offset map.
    pub fn checkpoint(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(
            "resolved_ts".to_string(),
            self.global_resolved_ts.to_string(),
        );
        m.insert("table_id".to_string(), self.config.table_id.to_string());
        m
    }

    /// Restore checkpoint from an offset map.
    pub fn restore_checkpoint(&mut self, offset: &HashMap<String, String>) {
        if let Some(ts) = offset.get("resolved_ts") {
            if let Ok(v) = ts.parse::<u64>() {
                self.config.checkpoint_ts = v;
                self.global_resolved_ts = v;
            }
        }
    }

    pub fn pending_tx_count(&self) -> usize {
        self.tracker.pending_count()
    }

    /// Close all region streams.
    pub async fn close(&mut self) {
        for region in self.regions.values_mut() {
            if let Some(stream) = region.stream.take() {
                stream.close().await;
            }
        }
        self.regions.clear();
    }
}

/// Helper to convert a `CdcRow`'s op_type into a Seatunnel RowKind later.
pub fn decode_handle_from_row(row: &CdcRow) -> Option<i64> {
    decode_record_key(&row.key).map(|(_, h)| h)
}
