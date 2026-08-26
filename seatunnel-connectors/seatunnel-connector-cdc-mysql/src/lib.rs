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

//! MySQL CDC (Change Data Capture) connector.
//!
//! ## Pipeline semantics
//!
//! ```text
//! open()
//!   ├─ connect + verify
//!   ├─ SHOW MASTER STATUS          → baseline (file, position, GTID set)
//!   ├─ start binlog dump @baseline ── events accumulate in a bounded buffer
//!   └─ enumerate snapshot splits   → [id_start, id_end) ranges
//!
//! poll_next()
//!   ├─ SNAPSHOT phase
//!   │    ├─ non-blocking drain of binlog events into the buffer
//!   │    │    (buffer full ⇒ TCP backpressure slows the dump; no loss)
//!   │    └─ keyset-paginated `SELECT … WHERE id ∈ [start,end) AND id > :last`
//!   └─ INCREMENTAL phase
//!        ├─ replay buffered changes first (Insert rows already covered by
//!        │    the snapshot — primary key ≤ snapshot max — are dropped)
//!        └─ then follow the live binlog stream forever
//! ```
//!
//! Delivery guarantees: **at-least-once**. With `parallelism > 1`, snapshot
//! ranges are partitioned across subtasks while exactly one designated
//! subtask (index 0) streams the binlog from its pre-snapshot baseline —
//! changes committed during the snapshot window are replayed on top of the
//! snapshot, so nothing is lost; a bounded number of duplicates inside the
//! overlap window is possible and expected. Checkpoint state serializes
//! phase, binlog file/position/GTID, split progress and the snapshot
//! high-watermark so a restarted task resumes where it stopped.
//!
//! ## Requirements
//! - MySQL 5.7+/8.0+, `binlog_format=ROW`, `binlog_row_image=FULL`
//! - User privileges: `SELECT`, `REPLICATION SLAVE`, `REPLICATION CLIENT`

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;

use futures::{Stream, StreamExt};
use mysql_async::{
    binlog::events::{Event, EventData, RowsEventData, TableMapEvent},
    binlog::row::BinlogRow,
    prelude::*,
    BinlogStream, BinlogStreamRequest, OptsBuilder, Pool, Row, Value,
};
use seatunnel_api::{
    row::{Field, Row as SeatunnelRow, RowKind},
    schema::TableSchema,
    source::{
        source_reader::{PollResult, SourceReader, SourceReaderContext},
        source_split::SourceSplit,
        source_split_enum::SourceSplitEnumeratorContext,
        Boundedness, Source,
    },
};
use seatunnel_connector_cdc_base::{
    CdcConfig, CdcPhase, CdcSource, CdcState, IncrementalSplit, SnapshotSplit, Watermark,
};
use seatunnel_connector_common::ConnectorConfig;
use serde::{Deserialize, Serialize};

/// Output row from MySQL CDC.
#[derive(Debug, Clone)]
pub struct MySqlCdcOutput(pub SeatunnelRow);

impl From<MySqlCdcOutput> for SeatunnelRow {
    fn from(val: MySqlCdcOutput) -> Self {
        val.0
    }
}

/// Combined CDC split type for MySQL.
#[derive(Debug, Clone)]
pub enum MySqlCdcSplit {
    Snapshot(SnapshotSplit),
    Incremental(IncrementalSplit),
}

impl SourceSplit for MySqlCdcSplit {
    fn split_id(&self) -> &str {
        match self {
            MySqlCdcSplit::Snapshot(s) => s.split_id(),
            MySqlCdcSplit::Incremental(s) => s.split_id(),
        }
    }

    fn partition_count(&self) -> usize {
        match self {
            MySqlCdcSplit::Snapshot(s) => s.partition_count(),
            MySqlCdcSplit::Incremental(s) => s.partition_count(),
        }
    }
}

/// Maximum number of change rows buffered while the snapshot phase runs.
/// When exceeded the reader stops draining and lets TCP backpressure slow
/// the binlog dump instead of growing memory unboundedly.
const MAX_BUFFERED_BINLOG_ROWS: usize = 65_536;

/// Number of rows fetched per keyset-paginated snapshot query.
const SNAPSHOT_BATCH_SIZE: i64 = 500;

/// Binlog offset for MySQL binlog streaming.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BinlogOffset {
    pub file: String,
    pub position: u64,
    pub gtid_set: Option<String>,
}

impl BinlogOffset {
    pub fn new(file: &str, position: u64) -> Self {
        BinlogOffset {
            file: file.to_string(),
            position,
            gtid_set: None,
        }
    }

    pub fn with_gtid(mut self, gtid: &str) -> Self {
        self.gtid_set = Some(gtid.to_string());
        self
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("file".to_string(), self.file.clone());
        m.insert("position".to_string(), self.position.to_string());
        if let Some(g) = &self.gtid_set {
            m.insert("gtid_set".to_string(), g.clone());
        }
        m
    }
}

/// MySQL CDC startup mode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MySqlStartupMode {
    /// Full snapshot followed by streaming (default).
    #[default]
    Initial,
    /// Snapshot only, stop after the snapshot completes.
    SnapshotOnly,
    /// Skip the snapshot; stream from the earliest retained binlog.
    Earliest,
    /// Skip the snapshot; stream from "now".
    Latest,
    Timestamp {
        timestamp: i64,
    },
    Specific {
        file: String,
        position: u64,
        gtid_set: Option<String>,
    },
}

/// MySQL CDC configuration.
#[derive(Debug, Clone)]
pub struct MySqlCdcConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database_name: String,
    pub table_name: String,
    pub startup_mode: MySqlStartupMode,
    pub parallelism: usize,
    pub server_timezone: String,
    pub server_id: u32,
    /// Split key column (defaults to `id`).
    pub split_column: String,
    /// This reader's subtask index / total subtask count — snapshot ranges
    /// are partitioned so each subtask scans a disjoint id interval.
    pub subtask_index: usize,
    pub subtask_count: usize,
}

impl Default for MySqlCdcConfig {
    fn default() -> Self {
        MySqlCdcConfig {
            hostname: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: String::new(),
            database_name: "seatunnel".to_string(),
            table_name: "users".to_string(),
            startup_mode: MySqlStartupMode::Initial,
            parallelism: 4,
            server_timezone: "+00:00".to_string(),
            server_id: 0,
            split_column: "id".to_string(),
            subtask_index: 0,
            subtask_count: 1,
        }
    }
}

impl MySqlCdcConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        MySqlCdcConfig {
            hostname: config.get_string("hostname", "localhost"),
            port: config.get_int("port", 3306) as u16,
            username: config.get_string("username", "root"),
            password: config.get_string("password", ""),
            database_name: config.get_string("database-name", "seatunnel"),
            table_name: config.get_string("table-name", "users"),
            parallelism: config.get_int("parallelism", 4) as usize,
            startup_mode: config
                .get("startup.mode")
                .map(|s| match s.as_str() {
                    "initial" => MySqlStartupMode::Initial,
                    "snapshot" | "snapshot-only" => MySqlStartupMode::SnapshotOnly,
                    "earliest" => MySqlStartupMode::Earliest,
                    "latest" => MySqlStartupMode::Latest,
                    _ => MySqlStartupMode::Initial,
                })
                .unwrap_or(MySqlStartupMode::Initial),
            server_timezone: config.get_string("server-timezone", "+00:00"),
            server_id: config.get_int("server-id", 0) as u32,
            split_column: config.get_string("split.column", "id"),
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
        }
    }

    /// Effective replication pseudo-server id, unique per dump connection.
    ///
    /// MySQL rejects a second replica that reuses a live server_id, so the
    /// value mixes process, port and a monotonically increasing counter —
    /// distinct across parallel readers *and* across reconnects.
    fn effective_server_id(&self) -> u32 {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        if self.server_id != 0 {
            return self.server_id;
        }
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let mixed = (self.port as u32).wrapping_mul(0x9E37_79B9)
            ^ (std::process::id())
            ^ n.wrapping_mul(0x85EB_CA6B)
            ^ nanos;
        // Keep well clear of typical server ids (0/1) and of the sign bit.
        (mixed % 0x0FFF_FFF0) + 0x1000
    }

    /// Extract the bare table name from a possibly `db.table` qualified value.
    fn bare_table(&self) -> &str {
        self.table_name
            .rsplit('.')
            .next()
            .unwrap_or(&self.table_name)
    }

    /// Does this table-name pattern match the given table?
    /// Supports a trailing `%` wildcard; otherwise exact match.
    fn table_matches(&self, table: &str) -> bool {
        let want = self.bare_table();
        let got = table;
        if let Some(prefix) = want.strip_suffix('%') {
            got.starts_with(prefix)
        } else {
            got == want
        }
    }
}

/// MySQL CDC Source.
#[derive(Debug, Clone)]
pub struct MySqlCdcSource {
    config: MySqlCdcConfig,
    cdc_config: CdcConfig,
    schema: Option<TableSchema>,
}

impl MySqlCdcSource {
    pub fn new(config: MySqlCdcConfig, schema: Option<TableSchema>) -> Self {
        let cdc_config = CdcConfig::new(
            &config.hostname,
            config.port,
            &config.username,
            &config.password,
            &config.database_name,
            &config.table_name,
        );
        MySqlCdcSource {
            config,
            cdc_config,
            schema,
        }
    }

    pub fn from_config(config: &ConnectorConfig, schema: Option<TableSchema>) -> Self {
        MySqlCdcSource::new(MySqlCdcConfig::from_config(config), schema)
    }

    fn build_pool(&self) -> Pool {
        Self::build_pool_for(&self.config)
    }

    fn build_pool_for(config: &MySqlCdcConfig) -> Pool {
        let opts = OptsBuilder::default()
            .ip_or_hostname(&config.hostname)
            .tcp_port(config.port)
            .user(Some(&config.username))
            .pass(Some(&config.password))
            .db_name(Some(&config.database_name));
        Pool::new(opts)
    }
}

impl CdcSource for MySqlCdcSource {
    fn config(&self) -> &CdcConfig {
        &self.cdc_config
    }

    fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }
}

impl Source for MySqlCdcSource {
    type Output = MySqlCdcOutput;
    type Split = MySqlCdcSplit;
    type State = CdcState;

    fn enumerate_splits(
        &self,
        context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        let pool = self.build_pool();
        let splits =
            tokio_block_on(async { enumerate_snapshot_splits(&pool, &self.config).await })?;
        let _ = context.parallelism;
        Ok(splits.into_iter().map(MySqlCdcSplit::Snapshot).collect())
    }

    fn create_reader(
        &self,
        _context: SourceReaderContext,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(MySqlCdcReader::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_reader(
        &self,
        _context: SourceReaderContext,
        state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        let mut reader = MySqlCdcReader::new(self.config.clone(), self.schema.clone());
        reader.apply_cdc_state(state.clone());
        Ok(Box::new(reader))
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded
    }
}

/// Enumerate snapshot splits from the live table: `[MIN(id), MAX(id))`
/// divided into `parallelism` chunks. Fails loudly when the table cannot be
/// read — silently guessing ranges would produce wrong data.
async fn enumerate_snapshot_splits(
    pool: &Pool,
    config: &MySqlCdcConfig,
) -> anyhow::Result<Vec<SnapshotSplit>> {
    let mut conn = pool.get_conn().await.map_err(|e| {
        anyhow::anyhow!(
            "MySQL CDC cannot connect to {}:{} as {}: {}",
            config.hostname,
            config.port,
            config.username,
            e
        )
    })?;
    let table_ref = format!("`{}`.`{}`", config.database_name, config.table_name);

    let min_max: Option<(Option<i64>, Option<i64>)> = conn
        .query_first(format!(
            "SELECT MIN(`{}`), MAX(`{}`) FROM {}",
            config.split_column, config.split_column, table_ref
        ))
        .await?;

    let (Some(min_id), Some(max_id)) = (min_max.and_then(|(a, _)| a), min_max.and_then(|(_, b)| b))
    else {
        tracing::info!(
            "MySQL CDC: table {} is empty; a single empty snapshot split is created",
            table_ref
        );
        return Ok(vec![SnapshotSplit::new(
            &config.database_name,
            &config.table_name,
            &config.split_column,
            "0",
            "1",
        )]);
    };

    let parallelism = config.subtask_count.max(1);
    let span = (max_id - min_id + 1).max(1);
    let chunk = (span + (parallelism as i64) - 1) / (parallelism as i64);
    // Each subtask scans a disjoint interval of the id space so parallel
    // readers never duplicate snapshot rows.
    let idx = (config.subtask_index.min(parallelism - 1)) as i64;
    let range_start = min_id + idx * chunk;
    let range_end = range_start.saturating_add(chunk).min(max_id + 1);
    if range_start > max_id {
        tracing::info!(
            "MySQL CDC: subtask {}/{} has an empty snapshot range",
            config.subtask_index,
            parallelism
        );
        return Ok(vec![SnapshotSplit::new(
            &config.database_name,
            &config.table_name,
            &config.split_column,
            "0",
            "1",
        )]);
    }
    let mut splits = Vec::new();
    let mut cursor = range_start;
    while cursor < range_end {
        let end = cursor.saturating_add(chunk).min(range_end);
        splits.push(SnapshotSplit::new(
            &config.database_name,
            &config.table_name,
            &config.split_column,
            &cursor.to_string(),
            &end.to_string(),
        ));
        cursor = end;
    }
    tracing::info!(
        "MySQL CDC: subtask {}/{} enumerated {} split(s) for {} covering ids [{}, {})",
        config.subtask_index,
        parallelism,
        splits.len(),
        table_ref,
        range_start,
        range_end
    );
    Ok(splits)
}

/// Run an async block even outside a tokio runtime (best-effort).
fn tokio_block_on<F: std::future::Future>(fut: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.block_on(fut),
        Err(_) => tokio::runtime::Runtime::new()
            .expect("spawn tokio runtime")
            .block_on(fut),
    }
}

/// A decoded change row waiting in the binlog replay buffer.
#[derive(Debug, Clone)]
struct BufferedChange {
    row: SeatunnelRow,
}

/// MySQL CDC Source reader.
pub struct MySqlCdcReader {
    config: MySqlCdcConfig,
    #[allow(dead_code)] // retained for future schema-aware serialization
    schema: Option<TableSchema>,
    phase: CdcPhase,
    splits: Vec<MySqlCdcSplit>,
    current_idx: Cell<usize>,
    /// Per-split keyset pagination cursor: split index → last emitted pk.
    split_last_pk: HashMap<usize, i64>,
    offset: BinlogOffset,
    watermark: Watermark,
    /// Highest primary key emitted by the snapshot phase (dedup boundary).
    max_snapshot_pk: i64,
    binlog_stream: Option<Pin<Box<BinlogStream>>>,
    /// Changes decoded from binlog events awaiting emission (ordered).
    binlog_buffer: VecDeque<BufferedChange>,
    /// Buffered rows from the current snapshot batch.
    snapshot_buffer: VecDeque<SeatunnelRow>,
    /// Cached table map events keyed by table id.
    table_maps: HashMap<u64, TableMapEvent<'static>>,
    /// True when the binlog stream hit a fatal error and needs re-establishing.
    stream_broken: bool,
}

impl MySqlCdcReader {
    pub fn new(config: MySqlCdcConfig, schema: Option<TableSchema>) -> Self {
        MySqlCdcReader {
            config,
            schema,
            phase: CdcPhase::Snapshot,
            splits: Vec::new(),
            current_idx: Cell::new(0),
            split_last_pk: HashMap::new(),
            offset: BinlogOffset::default(),
            watermark: Watermark::Min,
            max_snapshot_pk: i64::MIN,
            binlog_stream: None,
            binlog_buffer: VecDeque::new(),
            snapshot_buffer: VecDeque::new(),
            table_maps: HashMap::new(),
            stream_broken: false,
        }
    }

    fn build_pool(&self) -> Pool {
        MySqlCdcSource::build_pool_for(&self.config)
    }

    /// Apply previously snapshotted state (checkpoint restore path).
    pub fn apply_cdc_state(&mut self, state: CdcState) {
        self.phase = state.phase;
        self.watermark = state.watermark;

        if let Some(file) = state.offset.get("file") {
            self.offset.file = file.clone();
        }
        if let Some(pos) = state.offset.get("position") {
            self.offset.position = pos.parse().unwrap_or(0);
        }
        if let Some(gtid) = state.offset.get("gtid_set") {
            self.offset.gtid_set = Some(gtid.clone());
        }
        if let Some(idx) = state.offset.get("current_idx") {
            self.current_idx.set(idx.parse().unwrap_or(0));
        }
        if let Some(pk) = state.offset.get("max_snapshot_pk") {
            self.max_snapshot_pk = pk.parse().unwrap_or(i64::MIN);
        }
        if let Some(map) = state
            .offset
            .get("split_last_pk")
            .and_then(|s| serde_json::from_str::<HashMap<usize, i64>>(s).ok())
        {
            self.split_last_pk = map;
        }
        // Rebuild the exact split layout captured at checkpoint time so the
        // pagination cursors stay meaningful across restarts.
        if let Some(ranges) = state
            .offset
            .get("splits")
            .and_then(|s| serde_json::from_str::<Vec<(String, String)>>(s).ok())
        {
            self.splits = ranges
                .into_iter()
                .map(|(start, end)| {
                    MySqlCdcSplit::Snapshot(SnapshotSplit::new(
                        &self.config.database_name,
                        &self.config.table_name,
                        &self.config.split_column,
                        &start,
                        &end,
                    ))
                })
                .collect();
        }

        // An incremental-phase restore skips straight back to streaming.
        if self.phase == CdcPhase::Incremental {
            self.splits.clear();
        }

        tracing::info!(
            "MySQL CDC reader: restored state phase={} offset={}/{} splits={}",
            self.phase,
            self.offset.file,
            self.offset.position,
            self.splits.len()
        );
    }

    /// Deserialize + apply a serialized [`CdcState`].
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: CdcState =
            serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!("bad CDC state: {}", e))?;
        self.apply_cdc_state(state);
        Ok(())
    }

    async fn connect_and_prepare(&mut self) -> anyhow::Result<()> {
        let pool = self.build_pool();
        let mut conn = pool.get_conn().await.map_err(|e| {
            anyhow::anyhow!(
                "MySQL CDC cannot connect to {}:{} as {}: {}",
                self.config.hostname,
                self.config.port,
                self.config.username,
                e
            )
        })?;

        let _: Option<String> = conn
            .query_first("SELECT 1")
            .await?
            .ok_or_else(|| anyhow::anyhow!("MySQL ping returned no result"))?;

        // Resume positions only make sense when a previous state exists.
        let resuming_incremental =
            self.phase == CdcPhase::Incremental && !self.offset.file.is_empty();
        let resuming_snapshot = self.phase == CdcPhase::Snapshot && !self.offset.file.is_empty();

        if !resuming_incremental && !resuming_snapshot {
            let master_status = conn
                .query_first::<mysql_async::Row, _>("SHOW MASTER STATUS")
                .await?;
            let row = master_status.ok_or_else(|| {
                anyhow::anyhow!("SHOW MASTER STATUS empty — is the binlog enabled?")
            })?;
            if let Some(file) = row.get::<Option<String>, usize>(0).flatten() {
                self.offset.file = file;
            }
            if let Some(pos) = row.get::<Option<u64>, usize>(1).flatten() {
                self.offset.position = pos;
            }
            if let Some(gtid) = row.get::<Option<String>, usize>(4).flatten() {
                if !gtid.is_empty() {
                    self.offset.gtid_set = Some(gtid);
                }
            }
            tracing::info!(
                "MySQL CDC reader: baseline binlog offset {}/{} gtid={:?}",
                self.offset.file,
                self.offset.position,
                self.offset.gtid_set
            );
        } else {
            tracing::info!(
                "MySQL CDC reader: resuming from checkpoint offset {}/{}",
                self.offset.file,
                self.offset.position
            );
        }

        // Start streaming immediately from the recorded position so nothing
        // between the baseline and the end of the snapshot is lost.
        //
        // With parallelism > 1 only subtask 0 streams: MySQL allows one
        // replication stream per server_id and a single binlog parser is the
        // standard design (Debezium-style). Other subtasks are
        // snapshot-only; their ranges plus subtask 0's stream cover
        // everything with at-least-once delivery.
        let streams_binlog = !matches!(self.config.startup_mode, MySqlStartupMode::SnapshotOnly)
            && self.config.subtask_index == 0;
        if streams_binlog {
            match Self::open_binlog_stream(conn, &self.config, &self.offset).await {
                Ok(stream) => {
                    self.binlog_stream = Some(Box::pin(stream));
                    self.stream_broken = false;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "binlog stream failed (check REPLICATION SLAVE privilege, row format): {}",
                        e
                    ));
                }
            }
        }
        Ok(())
    }

    async fn open_binlog_stream(
        conn: mysql_async::Conn,
        config: &MySqlCdcConfig,
        offset: &BinlogOffset,
    ) -> anyhow::Result<BinlogStream> {
        let mut request = BinlogStreamRequest::new(config.effective_server_id());
        if !offset.file.is_empty() && offset.position > 0 {
            request = request
                .with_filename(offset.file.as_bytes())
                .with_pos(offset.position);
        }
        if let Some(ref gtid) = offset.gtid_set {
            let sids = parse_gtid_set(gtid);
            if !sids.is_empty() {
                request = request.with_gtid_set(sids);
            }
        }
        conn.get_binlog_stream(request)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Non-blockingly pull already-available binlog events into the replay
    /// buffer. Called during the snapshot phase so the socket keeps draining
    /// without stealing time from snapshot reads.
    async fn drain_binlog_nonblocking(&mut self) {
        if self.binlog_stream.is_none() || self.phase != CdcPhase::Snapshot {
            return;
        }
        while self.binlog_buffer.len() < MAX_BUFFERED_BINLOG_ROWS {
            // Single non-blocking attempt: map Pending to "nothing queued"
            // instead of awaiting the next event (that would stall snapshot
            // reads until the source table changes).
            use std::task::Poll;
            let poll_res = std::future::poll_fn(|cx| {
                let stream = self.binlog_stream.as_mut().unwrap();
                match Stream::poll_next(stream.as_mut(), cx) {
                    Poll::Ready(item) => Poll::Ready(Some(item)),
                    Poll::Pending => Poll::Ready(None),
                }
            })
            .await;
            match poll_res {
                Some(Some(Ok(event))) => {
                    tracing::trace!(
                        "MySQL CDC drain: event type={:?} pos={}",
                        event.header().event_type(),
                        event.header().log_pos()
                    );
                    self.absorb_event(event);
                }
                Some(Some(Err(e))) => {
                    tracing::warn!("MySQL CDC binlog stream error during snapshot: {}", e);
                    self.binlog_stream = None;
                    self.stream_broken = true;
                    return;
                }
                _ => return, // nothing queued right now
            }
        }
    }

    /// Decode one binlog event into the replay buffer, tracking offsets.
    fn absorb_event(&mut self, event: Event) {
        self.offset.position = event.header().log_pos() as u64;
        let data = match event.read_data() {
            Ok(Some(d)) => d,
            _ => return,
        };
        match data {
            EventData::TableMapEvent(tme) => {
                self.table_maps.insert(tme.table_id(), tme.into_owned());
            }
            EventData::RowsEvent(rows) => self.absorb_rows_event(rows),
            _ => {}
        }
    }

    fn absorb_rows_event(&mut self, rows: RowsEventData) {
        let table_id = rows.table_id();
        let num_cols = rows.num_columns() as usize;
        let Some(tme) = self.table_maps.get(&table_id) else {
            return;
        };
        // Only capture events belonging to the configured table.
        if !(tme.database_name().as_bytes() == self.config.database_name.as_bytes()
            && self.config.table_matches(tme.table_name().as_ref()))
        {
            return;
        }

        let mut decoded: VecDeque<BufferedChange> = VecDeque::new();
        match &rows {
            RowsEventData::WriteRowsEvent(e) => {
                for pair in e.rows(tme) {
                    if let Ok((_, Some(after))) = pair {
                        decoded.push_back(BufferedChange {
                            row: binlog_row_to_seatunnel(&after, num_cols, RowKind::Insert),
                        });
                    }
                }
            }
            RowsEventData::UpdateRowsEvent(e) => {
                for pair in e.rows(tme) {
                    if let Ok((Some(before), Some(after))) = pair {
                        decoded.push_back(BufferedChange {
                            row: binlog_row_to_seatunnel(&before, num_cols, RowKind::Delete),
                        });
                        decoded.push_back(BufferedChange {
                            row: binlog_row_to_seatunnel(&after, num_cols, RowKind::Insert),
                        });
                    }
                }
            }
            RowsEventData::DeleteRowsEvent(e) => {
                for pair in e.rows(tme) {
                    if let Ok((Some(before), _)) = pair {
                        decoded.push_back(BufferedChange {
                            row: binlog_row_to_seatunnel(&before, num_cols, RowKind::Delete),
                        });
                    }
                }
            }
            _ => {}
        }
        self.binlog_buffer.extend(decoded);
    }

    /// Blocking-with-timeout read used in the incremental phase.
    async fn next_binlog_event_with_timeout(
        &mut self,
        timeout_ms: u64,
    ) -> anyhow::Result<Option<()>> {
        let Some(stream) = self.binlog_stream.as_mut() else {
            return Ok(None);
        };
        match tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            stream.as_mut().next(),
        )
        .await
        {
            Ok(Some(Ok(event))) => {
                self.absorb_event(event);
                Ok(Some(()))
            }
            Ok(Some(Err(e))) => {
                tracing::error!("MySQL CDC binlog stream failed: {}", e);
                self.binlog_stream = None;
                self.stream_broken = true;
                Err(anyhow::anyhow!("binlog stream error: {}", e))
            }
            Ok(None) => {
                // Server closed the dump stream; force a reconnect next poll.
                self.binlog_stream = None;
                self.stream_broken = true;
                Ok(None)
            }
            Err(_) => Ok(None), // timeout, no events yet
        }
    }

    /// Re-establish a broken binlog stream from the last known position.
    async fn maybe_reconnect_stream(&mut self) -> anyhow::Result<()> {
        if !self.stream_broken || self.binlog_stream.is_some() {
            return Ok(());
        }
        tracing::info!(
            "MySQL CDC reader: reconnecting binlog stream from {}/{}",
            self.offset.file,
            self.offset.position
        );
        let pool = self.build_pool();
        let conn = pool.get_conn().await?;
        let stream = Self::open_binlog_stream(conn, &self.config, &self.offset).await?;
        self.binlog_stream = Some(Box::pin(stream));
        self.stream_broken = false;
        Ok(())
    }

    /// Pop the next change from the replay buffer.
    ///
    /// No value-based suppression is applied: changes buffered during the
    /// snapshot window are replayed verbatim, giving clean **at-least-once**
    /// semantics without risking loss on rows that landed in snapshot gaps.
    fn next_buffered_change(&mut self) -> Option<SeatunnelRow> {
        self.binlog_buffer.pop_front().map(|change| change.row)
    }

    fn note_snapshot_pk(&mut self, row: &SeatunnelRow) {
        if let Some(pk) = Some(row.get(0)).and_then(field_to_i64) {
            if pk > self.max_snapshot_pk {
                self.max_snapshot_pk = pk;
            }
        }
    }
}

impl SourceReader for MySqlCdcReader {
    type Output = MySqlCdcOutput;
    type Split = MySqlCdcSplit;

    fn open(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!(
                "MySQL CDC reader opening: {}.{} mode={:?}",
                self.config.database_name,
                self.config.table_name,
                self.config.startup_mode
            );

            self.connect_and_prepare().await?;

            // Startup-mode shortcuts.
            match self.config.startup_mode {
                MySqlStartupMode::Latest | MySqlStartupMode::Earliest => {
                    // Streaming-only: skip the snapshot entirely.
                    self.phase = CdcPhase::Incremental;
                    self.splits.clear();
                    tracing::info!(
                        "MySQL CDC reader: skipping snapshot ({:?})",
                        self.config.startup_mode
                    );
                    return Ok(());
                }
                MySqlStartupMode::Specific {
                    ref file,
                    position,
                    ref gtid_set,
                } => {
                    self.offset.file = file.clone();
                    self.offset.position = position;
                    self.offset.gtid_set = gtid_set.clone();
                    self.phase = CdcPhase::Incremental;
                    self.splits.clear();
                    // Re-open the stream from the requested position.
                    self.binlog_stream = None;
                    self.stream_broken = false;
                    let pool = self.build_pool();
                    let conn = pool.get_conn().await?;
                    let stream = Self::open_binlog_stream(conn, &self.config, &self.offset).await?;
                    self.binlog_stream = Some(Box::pin(stream));
                    return Ok(());
                }
                MySqlStartupMode::Timestamp { .. } => {
                    // Timestamp resume requires binlog timestamp scanning;
                    // fall back to a full snapshot + stream from now.
                    tracing::warn!(
                        "MySQL CDC: startup.mode=timestamp not supported yet, using initial"
                    );
                }
                MySqlStartupMode::SnapshotOnly | MySqlStartupMode::Initial => {}
            }

            // Auto-seed snapshot splits unless a checkpoint already supplied them.
            if self.splits.is_empty() {
                let pool = self.build_pool();
                let fresh = enumerate_snapshot_splits(&pool, &self.config).await?;
                self.splits = fresh.into_iter().map(MySqlCdcSplit::Snapshot).collect();
            }

            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>,
    > {
        Box::pin(async move {
            if self.phase == CdcPhase::Incremental {
                return self.poll_incremental().await;
            }

            // ---- Snapshot phase ---------------------------------------
            tracing::trace!(
                "MySQL CDC poll[snapshot]: buffered_binlog={} snapshot_buf={} idx={}/{}",
                self.binlog_buffer.len(),
                self.snapshot_buffer.len(),
                self.current_idx.get(),
                self.splits.len()
            );
            // Keep the binlog socket drained while chunk-scanning the table.
            self.drain_binlog_nonblocking().await;

            // Emit buffered snapshot rows first.
            if let Some(row) = self.snapshot_buffer.pop_front() {
                self.note_snapshot_pk(&row);
                return Ok(PollResult::Record(MySqlCdcOutput(row)));
            }

            let idx = self.current_idx.get();
            if idx >= self.splits.len() {
                // All snapshot splits consumed. Subtask 0 transitions to the
                // incremental stream; auxiliary subtasks are snapshot-only
                // and finish (their changes are covered by subtask 0).
                if self.config.subtask_index != 0 {
                    tracing::info!(
                        "MySQL CDC reader: subtask {}/{} snapshot complete, EOF (streaming handled by subtask 0)",
                        self.config.subtask_index,
                        self.config.subtask_count
                    );
                    return Ok(PollResult::EOF);
                }
                self.handle_no_more_splits();
                if self.config.startup_mode == MySqlStartupMode::SnapshotOnly {
                    tracing::info!("MySQL CDC reader: snapshot-only mode, EOF");
                    return Ok(PollResult::EOF);
                }
                return Ok(PollResult::Empty);
            }

            let Some(MySqlCdcSplit::Snapshot(split)) = self.splits.get(idx) else {
                self.current_idx.set(idx + 1);
                return Ok(PollResult::Empty);
            };
            let split = split.clone();

            let last_pk = self.split_last_pk.get(&idx).copied().unwrap_or(0);
            tracing::trace!(
                "MySQL CDC poll[snapshot]: querying split {} range [{}, {}) after pk {}",
                idx,
                split.start_key,
                split.end_key,
                last_pk
            );
            let rows = query_snapshot_batch(&self.config, &split, last_pk).await?;
            tracing::trace!(
                "MySQL CDC poll[snapshot]: batch returned {} rows",
                rows.len()
            );
            if rows.is_empty() {
                self.current_idx.set(idx + 1);
                return Ok(PollResult::Empty);
            }
            let end = rows[rows.len() - 1].0;
            for (_, row) in rows {
                self.snapshot_buffer.push_back(row);
            }
            // Advance the keyset cursor to the LAST pk of this batch so the
            // next query resumes strictly after what was already emitted.
            self.split_last_pk.insert(idx, end);
            tracing::trace!(
                "MySQL CDC poll[snapshot]: split {} cursor advanced to pk {} (map={:?})",
                idx,
                end,
                self.split_last_pk
            );
            if let Some(row) = self.snapshot_buffer.pop_front() {
                self.note_snapshot_pk(&row);
                return Ok(PollResult::Record(MySqlCdcOutput(row)));
            }
            Ok(PollResult::Empty)
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let mut offset = self.offset.to_hashmap();
        offset.insert(
            "current_idx".to_string(),
            self.current_idx.get().to_string(),
        );
        offset.insert(
            "max_snapshot_pk".to_string(),
            self.max_snapshot_pk.to_string(),
        );
        if !self.split_last_pk.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.split_last_pk) {
                offset.insert("split_last_pk".to_string(), json);
            }
        }
        let ranges: Vec<(String, String)> = self
            .splits
            .iter()
            .filter_map(|s| match s {
                MySqlCdcSplit::Snapshot(sp) => Some((sp.start_key.clone(), sp.end_key.clone())),
                MySqlCdcSplit::Incremental(_) => None,
            })
            .collect();
        if !ranges.is_empty() {
            if let Ok(json) = serde_json::to_string(&ranges) {
                offset.insert("splits".to_string(), json);
            }
        }
        let state = CdcState {
            phase: self.phase,
            watermark: self.watermark,
            offset,
        };
        Box::pin(async move { serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e)) })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!("MySQL CDC reader: adding {} splits", splits.len());
        self.splits.extend(splits);
    }

    fn handle_no_more_splits(&mut self) {
        self.phase = CdcPhase::Incremental;
        self.watermark = Watermark::Value(self.max_snapshot_pk.max(0));
        tracing::info!(
            "MySQL CDC reader: transitioning to incremental phase (replay buffer={}, dedup below pk={})",
            self.binlog_buffer.len(),
            self.max_snapshot_pk
        );
    }

    fn close(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.binlog_stream = None;
            tracing::info!("MySQL CDC reader closed");
            Ok(())
        })
    }
}

impl MySqlCdcReader {
    /// Incremental phase pump: replay buffer → live stream → Empty.
    async fn poll_incremental(&mut self) -> anyhow::Result<PollResult<MySqlCdcOutput>> {
        // 1. Anything already decoded?
        if let Some(row) = self.next_buffered_change() {
            return Ok(PollResult::Record(MySqlCdcOutput(row)));
        }

        // 2. Recover from stream failures before reading further.
        if self.stream_broken {
            self.maybe_reconnect_stream().await?;
            if self.binlog_stream.is_none() {
                return Ok(PollResult::Empty);
            }
        }

        // 3. Read one event (bounded block) and try again.
        self.next_binlog_event_with_timeout(250).await?;
        if let Some(row) = self.next_buffered_change() {
            return Ok(PollResult::Record(MySqlCdcOutput(row)));
        }
        Ok(PollResult::Empty)
    }
}

/// Fetch the next keyset-paginated snapshot batch for a split.
/// Returns `(first_pk_of_batch, row)` pairs ordered by the split column.
async fn query_snapshot_batch(
    config: &MySqlCdcConfig,
    split: &SnapshotSplit,
    last_pk: i64,
) -> anyhow::Result<Vec<(i64, SeatunnelRow)>> {
    let pool = MySqlCdcSource::build_pool_for(config);
    let mut conn = pool.get_conn().await.map_err(|e| {
        anyhow::anyhow!(
            "snapshot query failed ({}:{}): {}",
            config.hostname,
            config.port,
            e
        )
    })?;

    let start_id: i64 = split.start_key.parse().unwrap_or(0);
    let end_id: i64 = split.end_key.parse().unwrap_or(i64::MAX);
    let sql = format!(
        "SELECT * FROM `{}`.`{}` WHERE `{}` >= {} AND `{}` < {} AND `{}` > {} \
         ORDER BY `{}` ASC LIMIT {}",
        split.database,
        split.table,
        split.split_column,
        start_id,
        split.split_column,
        end_id,
        split.split_column,
        last_pk,
        split.split_column,
        SNAPSHOT_BATCH_SIZE,
    );
    // `exec` uses the binary protocol so numeric/temporal columns arrive
    // fully typed instead of as raw byte strings.
    let rows: Vec<Row> = conn.exec(sql, ()).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let pk = extract_pk(r, last_pk);
            (pk, mysql_row_to_seatunnel_row(r))
        })
        .collect())
}

/// Pull an i64 split-key value out of the first column of a snapshot row.
/// Handles the binary-protocol numeric variants and falls back to parsing
/// textual representations.
fn extract_pk(row: &Row, default_pk: i64) -> i64 {
    match row.get::<Value, usize>(0) {
        Some(Value::Int(i)) => i,
        Some(Value::UInt(u)) => i64::try_from(u).unwrap_or(default_pk),
        Some(Value::Float(f)) => f as i64,
        Some(Value::Double(d)) => d as i64,
        Some(Value::Bytes(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(default_pk),
        _ => default_pk,
    }
}

/// Converts a mysql_async Row to a seatunnel Row.
fn mysql_row_to_seatunnel_row(mysql_row: &Row) -> SeatunnelRow {
    let field_count = mysql_row.len();
    let mut row = SeatunnelRow::new(RowKind::Insert, field_count);
    for col_idx in 0..field_count {
        row.set(col_idx, mysql_value_to_field(mysql_row, col_idx));
    }
    row
}

/// Converts a single column value from a mysql_async Row to a seatunnel Field.
fn mysql_value_to_field(row: &Row, col_idx: usize) -> Field {
    match row.get::<Value, usize>(col_idx) {
        None | Some(Value::NULL) => Field::Null,
        Some(Value::Int(v)) => Field::Int64(v),
        Some(Value::UInt(v)) => Field::UInt64(v),
        Some(Value::Float(v)) => Field::Float32(v),
        Some(Value::Double(v)) => Field::Float64(v),
        Some(Value::Bytes(v)) => bytes_to_field(v),
        Some(Value::Date(y, m, d, _, _, _, _)) => {
            Field::String(format!("{:04}-{:02}-{:02}", y, m, d))
        }
        Some(Value::Time(_, _, h, m, s, _)) => Field::String(format!("{:02}:{:02}:{:02}", h, m, s)),
    }
}

/// Decode bytes as UTF-8 text when possible (VARCHAR/TEXT columns);
/// keep raw bytes for binary payloads (BLOB).
fn bytes_to_field(bytes: Vec<u8>) -> Field {
    match String::from_utf8(bytes) {
        Ok(s) => Field::String(s),
        Err(b) => Field::Bytes(b.into_bytes()),
    }
}

fn field_to_i64(field: &Field) -> Option<i64> {
    match field {
        Field::Int8(v) => Some(*v as i64),
        Field::Int16(v) => Some(*v as i64),
        Field::Int32(v) => Some(*v as i64),
        Field::Int64(v) => Some(*v),
        Field::UInt8(v) => Some(*v as i64),
        Field::UInt16(v) => Some(*v as i64),
        Field::UInt32(v) => Some(*v as i64),
        Field::UInt64(v) => i64::try_from(*v).ok(),
        Field::Float32(v) => Some(*v as i64),
        Field::Float64(v) => Some(*v as i64),
        Field::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Convert a BinlogRow to a SeatunnelRow.
fn binlog_row_to_seatunnel(row: &BinlogRow, _num_cols: usize, kind: RowKind) -> SeatunnelRow {
    let field_count = row.len();
    let mut seatunnel_row = SeatunnelRow::new(kind, field_count);
    for idx in 0..field_count {
        if let Some(bv) = row.as_ref(idx) {
            seatunnel_row.set(idx, binlog_value_to_field(bv));
        } else {
            seatunnel_row.set(idx, Field::Null);
        }
    }
    seatunnel_row
}

/// Convert a BinlogValue to a SeatunnelField.
fn binlog_value_to_field(bv: &mysql_async::binlog::value::BinlogValue) -> Field {
    match bv {
        mysql_async::binlog::value::BinlogValue::Value(v) => value_to_field(v),
        mysql_async::binlog::value::BinlogValue::Jsonb(j) => {
            Field::String(binlog_jsonb_to_string(j))
        }
        mysql_async::binlog::value::BinlogValue::JsonDiff(_) => Field::Null,
    }
}

/// Convert a binlog JSONB value to a JSON string representation.
fn binlog_jsonb_to_string(v: &mysql_async::binlog::jsonb::Value) -> String {
    match v {
        mysql_async::binlog::jsonb::Value::Null => "null".to_string(),
        mysql_async::binlog::jsonb::Value::Bool(b) => b.to_string(),
        mysql_async::binlog::jsonb::Value::I16(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::U16(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::I32(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::U32(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::I64(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::U64(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::F64(n) => n.to_string(),
        mysql_async::binlog::jsonb::Value::String(s) => {
            let text = s.str();
            format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
        }
        _ => "[jsonb]".to_string(),
    }
}

/// Convert a mysql_async Value to a SeatunnelField (for snapshot phase).
fn value_to_field(v: &Value) -> Field {
    match v {
        Value::NULL => Field::Null,
        Value::Int(i) => Field::Int64(*i),
        Value::UInt(u) => Field::UInt64(*u),
        Value::Float(f) => Field::Float32(*f),
        Value::Double(d) => Field::Float64(*d),
        Value::Bytes(b) => bytes_to_field(b.clone()),
        Value::Date(y, m, d, _, _, _, _) => Field::String(format!("{:04}-{:02}-{:02}", y, m, d)),
        Value::Time(_, _, h, m, s, _) => Field::String(format!("{:02}:{:02}:{:02}", h, m, s)),
    }
}

/// Parse a MySQL GTID set string (`uuid:1-10:20,uuid2:5`) into `Vec<Sid>`
/// for the binlog dump request.
fn parse_gtid_set(gtid_set: &str) -> Vec<mysql_async::Sid<'_>> {
    use mysql_async::{GnoInterval, Sid};
    let mut sids = Vec::new();
    for part in gtid_set.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Format: <uuid>:<n1>[-<m1>][:<n2>[-<m2>]...]
        let mut segments = part.split(':');
        let uuid_hex = match segments.next() {
            Some(u) => u,
            None => continue,
        };
        // Parse the 36-char UUID "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" into 16 bytes.
        let compact: String = uuid_hex.chars().filter(|c| *c != '-').collect();
        if compact.len() != 32 {
            continue;
        }
        let mut uuid = [0u8; 16];
        let mut ok = true;
        for i in 0..16 {
            match u8::from_str_radix(&compact[i * 2..i * 2 + 2], 16) {
                Ok(b) => uuid[i] = b,
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }
        let mut sid = Sid::new(uuid);
        for interval in segments {
            let (start, end) = match interval.split_once('-') {
                Some((s, e)) => (s, e),
                None => (interval, interval),
            };
            let Ok(s) = start.parse::<u64>() else {
                continue;
            };
            let Ok(e) = end.parse::<u64>() else { continue };
            // GnoInterval is [start, end); a single GNO is [n, n+1).
            if let Ok(gno) = GnoInterval::check_and_new(s, e + 1) {
                sid = sid.with_interval(gno);
            }
        }
        sids.push(sid);
    }
    sids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MySqlCdcConfig {
        MySqlCdcConfig {
            hostname: "127.0.0.1".into(),
            port: 13306, // unreachable in tests
            username: "root".into(),
            password: String::new(),
            database_name: "testdb".into(),
            table_name: "test_table".into(),
            startup_mode: MySqlStartupMode::Initial,
            parallelism: 1,
            server_timezone: "+00:00".into(),
            server_id: 0,
            split_column: "id".into(),
            subtask_index: 0,
            subtask_count: 1,
        }
    }

    #[test]
    fn test_mysql_config_parsing() {
        let mut props = HashMap::new();
        props.insert("hostname".to_string(), "db-host".to_string());
        props.insert("port".to_string(), "3307".to_string());
        props.insert("database-name".to_string(), "mydb".to_string());
        props.insert("table-name".to_string(), "orders".to_string());
        props.insert("startup.mode".to_string(), "initial".to_string());
        props.insert("split.column".to_string(), "order_id".to_string());
        let config = ConnectorConfig::new(props);
        let mysql_config = MySqlCdcConfig::from_config(&config);
        assert_eq!(mysql_config.hostname, "db-host");
        assert_eq!(mysql_config.port, 3307);
        assert_eq!(mysql_config.database_name, "mydb");
        assert_eq!(mysql_config.table_name, "orders");
        assert_eq!(mysql_config.startup_mode, MySqlStartupMode::Initial);
        assert_eq!(mysql_config.split_column, "order_id");
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let offset = BinlogOffset::new("binlog.000001", 42).with_gtid("a1-b2-c3");
        assert_eq!(offset.file, "binlog.000001");
        assert_eq!(offset.position, 42);
        assert_eq!(offset.gtid_set, Some("a1-b2-c3".to_string()));
        let hm = offset.to_hashmap();
        assert_eq!(hm.get("file").map(|v| v.as_str()), Some("binlog.000001"));
    }

    #[test]
    fn test_parse_gtid_set() {
        // Single UUID with two intervals.
        let sids = parse_gtid_set("3a1e6a42-5f8e-11ec-bf63-0242ac130002:1-5:10");
        assert_eq!(sids.len(), 1);
        assert_eq!(sids[0].intervals().len(), 2);

        // Multiple UUIDs.
        let sids = parse_gtid_set(
            "3a1e6a42-5f8e-11ec-bf63-0242ac130002:1-5,b8d1a2c0-1111-2222-3333-444455556666:7",
        );
        assert_eq!(sids.len(), 2);
        // Single GNO -> interval [7, 8).
        assert_eq!(sids[1].intervals().len(), 1);

        // Malformed input is ignored.
        assert!(parse_gtid_set("not-a-gtid").is_empty());
        assert!(parse_gtid_set("").is_empty());
    }

    #[tokio::test]
    async fn snapshot_poll_fails_without_database() {
        // No synthetic fallback: an unreachable database must surface as an
        // error so the engine can fail/retry the task.
        let mut reader = MySqlCdcReader::new(test_config(), None);
        reader.add_splits(vec![MySqlCdcSplit::Snapshot(SnapshotSplit::new(
            "testdb",
            "test_table",
            "id",
            "0",
            "1000",
        ))]);
        let result = reader.poll_next().await;
        assert!(result.is_err(), "expected error, got {:?}", result);
    }

    #[tokio::test]
    async fn state_roundtrip_preserves_progress() {
        let mut reader = MySqlCdcReader::new(test_config(), None);
        reader.add_splits(vec![
            MySqlCdcSplit::Snapshot(SnapshotSplit::new("db", "tbl", "id", "0", "100")),
            MySqlCdcSplit::Snapshot(SnapshotSplit::new("db", "tbl", "id", "100", "200")),
        ]);
        reader.current_idx.set(1);
        reader.split_last_pk.insert(0, 77);
        reader.max_snapshot_pk = 88;
        reader.offset = BinlogOffset::new("binlog.000004", 1234).with_gtid("uuid:1-9");

        let bytes = reader.snapshot_state().await.unwrap();

        let mut restored = MySqlCdcReader::new(test_config(), None);
        restored.restore_from_state_bytes(&bytes).unwrap();
        assert_eq!(restored.current_idx.get(), 1);
        assert_eq!(restored.split_last_pk.get(&0), Some(&77));
        assert_eq!(restored.max_snapshot_pk, 88);
        assert_eq!(restored.offset.file, "binlog.000004");
        assert_eq!(restored.offset.position, 1234);
        assert_eq!(restored.splits.len(), 2);
    }

    #[tokio::test]
    async fn incremental_restore_skips_snapshot() {
        let mut reader = MySqlCdcReader::new(test_config(), None);
        reader.add_splits(vec![MySqlCdcSplit::Snapshot(SnapshotSplit::new(
            "db", "tbl", "id", "0", "100",
        ))]);
        assert!(!reader.splits.is_empty());

        let state = CdcState {
            phase: CdcPhase::Incremental,
            watermark: Watermark::Min,
            offset: BinlogOffset::new("binlog.000007", 4321).to_hashmap(),
        };
        reader.apply_cdc_state(state);
        assert_eq!(reader.phase, CdcPhase::Incremental);
        assert!(
            reader.splits.is_empty(),
            "incremental restore must skip snapshot splits"
        );
        assert_eq!(reader.offset.file, "binlog.000007");
    }

    #[test]
    fn replay_buffer_preserves_all_changes() {
        let mut reader = MySqlCdcReader::new(test_config(), None);
        reader.max_snapshot_pk = 50;

        let mut covered = BufferedChange {
            row: SeatunnelRow::new(RowKind::Insert, 1),
        };
        covered.row.set(0, Field::Int64(30));
        let mut beyond = BufferedChange {
            row: SeatunnelRow::new(RowKind::Insert, 1),
        };
        beyond.row.set(0, Field::Int64(80));
        let mut delete = BufferedChange {
            row: SeatunnelRow::new(RowKind::Delete, 1),
        };
        delete.row.set(0, Field::Int64(30));

        // At-least-once: the replay buffer is emitted verbatim, no
        // value-based suppression that could silently drop rows landing in
        // snapshot gaps.
        reader.binlog_buffer.push_back(covered);
        reader.binlog_buffer.push_back(beyond);
        reader.binlog_buffer.push_back(delete);

        assert_eq!(reader.binlog_buffer.len(), 3);
        assert!(reader.next_buffered_change().is_some());
        assert!(reader.next_buffered_change().is_some());
        assert!(reader.next_buffered_change().is_some());
        assert!(reader.next_buffered_change().is_none());
    }

    #[test]
    fn effective_server_id_unique_per_call() {
        let mut cfg = test_config();
        cfg.server_id = 0;
        // Distinct dump connections must never share a replication id.
        let ids: std::collections::HashSet<u32> =
            (0..16).map(|_| cfg.effective_server_id()).collect();
        assert_eq!(ids.len(), 16);
        assert!(ids
            .iter()
            .all(|id| *id >= 0x1000 && *id < 0x1000 + 0x0FFF_FFF0));
        // Explicit configuration always wins.
        cfg.server_id = 42;
        assert_eq!(cfg.effective_server_id(), 42);
    }

    #[test]
    fn table_pattern_matching() {
        let mut cfg = test_config();
        assert!(cfg.table_matches("test_table"));
        assert!(!cfg.table_matches("other"));
        cfg.table_name = "events%".into();
        assert!(cfg.table_matches("events_2026"));
        assert!(!cfg.table_matches("orders"));
        cfg.table_name = "mydb.users".into();
        assert!(cfg.table_matches("users"));
    }
}
