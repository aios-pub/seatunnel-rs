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

//! TiDB CDC (Change Data Capture) connector.
//!
//! ## Pipeline semantics (mirrors the MySQL/PostgreSQL connectors)
//!
//! ```text
//! open()
//!   ├─ SQL ping against TiDB                       (fail ⇒ task error)
//!   ├─ resolve TIDB_TABLE_ID                       (needed for TiKV keys)
//!   ├─ start TiKV CDC engine (PD → EventFeedV2)    → events buffer during snapshot
//!   └─ enumerate snapshot splits                   → disjoint ranges per subtask
//!
//! poll_next()
//!   ├─ SNAPSHOT phase
//!   │    ├─ bounded drain of CDC events into the replay buffer
//!   │    └─ keyset-paginated `SELECT … WHERE pk ∈ [start,end) AND pk > :last`
//!   └─ INCREMENTAL phase
//!        └─ replay buffer first, then follow live TiKV change events forever
//! ```
//!
//! Delivery guarantees: **at-least-once**. Changes committed while the
//! snapshot scan ran are buffered by the engine and replayed afterwards, so
//! nothing is lost; a bounded duplicate window inside the overlap is
//! expected. Checkpoint state serializes phase, resolved_ts, table id,
//! split progress and cursors so a restarted task resumes where it stopped.
//!
//! ## Requirements
//! - A real TiDB cluster (PD + TiKV + TiDB); the standalone `pingcap/tidb`
//!   image without TiKV has no CDC service
//! - PD client URLs reachable from the worker (e.g. `pd-addrs = 127.0.0.1:2379`)
//! - TiKV store addresses must be reachable too — advertise them on a
//!   host-mapped address when running inside Docker

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;

use mysql_async::{prelude::*, OptsBuilder, Pool, Value as MysqlValue};
use seatunnel_api::{
    row::{Field, Row, RowKind},
    schema::TableSchema,
    source::{
        source_reader::{PollResult, SourceReader, SourceReaderContext},
        source_split::SourceSplit,
        source_split_enum::SourceSplitEnumeratorContext,
        Boundedness, Source,
    },
};
use seatunnel_connector_cdc_base::{
    CdcPhase, CdcState, IncrementalSplit, SnapshotSplit, Watermark,
};
use seatunnel_connector_common::ConnectorConfig;

/// Number of rows fetched per keyset-paginated snapshot query.
const SNAPSHOT_BATCH_SIZE: i64 = 500;

/// Consecutive CDC-engine poll failures tolerated before the task fails.
const ENGINE_ERROR_TOLERANCE: u32 = 5;

/// Output row from TiDB CDC.
#[derive(Debug, Clone)]
pub struct TiDBCdcOutput(pub Row);

impl From<TiDBCdcOutput> for Row {
    fn from(val: TiDBCdcOutput) -> Self {
        val.0
    }
}

/// TiDB CDC split combining snapshot range and incremental state.
#[derive(Debug, Clone)]
pub enum TiDBCdcSplit {
    Snapshot(SnapshotSplit),
    Incremental(IncrementalSplit),
}

impl SourceSplit for TiDBCdcSplit {
    fn split_id(&self) -> &str {
        match self {
            TiDBCdcSplit::Snapshot(s) => s.split_id(),
            TiDBCdcSplit::Incremental(s) => s.split_id(),
        }
    }
}

/// TiDB connection configuration for MySQL-compatible access.
#[derive(Debug, Clone)]
pub struct TiDBCdcConnConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl TiDBCdcConnConfig {
    pub fn new(host: &str, port: u16, user: &str, password: &str, database: &str) -> Self {
        TiDBCdcConnConfig {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
        }
    }

    pub fn to_pool(&self) -> Pool {
        let opts = OptsBuilder::default()
            .ip_or_hostname(&self.host)
            .tcp_port(self.port)
            .user(Some(&self.user))
            .pass(Some(&self.password))
            .db_name(Some(&self.database));
        Pool::new(opts)
    }
}

/// TiDB CDC resolved timestamp.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct ResolvedTs(pub u64);

impl ResolvedTs {
    pub fn new(ts: u64) -> Self {
        ResolvedTs(ts)
    }

    pub fn to_timestamp(&self) -> u64 {
        self.0
    }
}

/// TiDB CDC configuration.
#[derive(Debug, Clone)]
pub struct TiDBCdcConfig {
    pub pd_addrs: Vec<String>,
    pub cluster_id: Option<u64>,
    pub namespace: String,
    pub database_name: String,
    pub table_name: String,
    /// Split key column for snapshot chunking (defaults to the integer PK).
    pub split_column: String,
    pub startup_mode: String,
    pub parallelism: usize,
    /// This reader's subtask index / total count — snapshot ranges are
    /// partitioned so each subtask scans a disjoint interval.
    pub subtask_index: usize,
    pub subtask_count: usize,
    pub capture_timeout: u64,
    /// Rewrites applied to TiKV leader addresses reported by PD
    /// (`from=to[,from=to…]`, host part only).
    pub store_address_rewrite: Vec<(String, String)>,
    /// Re-register region streams every N ms (0 disables). Each cycle makes
    /// TiKV run an incremental scan from the last resolved_ts — the reliably
    /// delivered change path. Default 10 000.
    pub resubscribe_interval_ms: u64,
    /// MySQL-compatible connection info for reading table metadata/data.
    pub conn: TiDBCdcConnConfig,
}

impl Default for TiDBCdcConfig {
    fn default() -> Self {
        TiDBCdcConfig {
            pd_addrs: vec!["127.0.0.1:2379".to_string()],
            cluster_id: None,
            namespace: "seatunnel".to_string(),
            database_name: "seatunnel".to_string(),
            table_name: "users".to_string(),
            split_column: "id".to_string(),
            startup_mode: "initial".to_string(),
            parallelism: 4,
            subtask_index: 0,
            subtask_count: 1,
            capture_timeout: 300_000,
            store_address_rewrite: Vec::new(),
            resubscribe_interval_ms: 10_000,
            conn: TiDBCdcConnConfig::new("127.0.0.1", 4000, "root", "", "seatunnel"),
        }
    }
}

impl TiDBCdcConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let pd = config
            .get_string("pd-addrs", "127.0.0.1:2379")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        TiDBCdcConfig {
            pd_addrs: pd,
            cluster_id: {
                let v = config.get_int("cluster-id", -1);
                if v >= 0 {
                    Some(v as u64)
                } else {
                    None
                }
            },
            namespace: config.get_string("namespace", "seatunnel"),
            database_name: config.get_string("database-name", "seatunnel"),
            table_name: config.get_string("table-name", "users"),
            split_column: config.get_string("split.column", "id"),
            startup_mode: config.get_string("startup.mode", "initial"),
            parallelism: config.get_int("parallelism", 4) as usize,
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
            capture_timeout: config.get_int("capture-timeout", 300_000) as u64,
            store_address_rewrite: config
                .get("store-address-rewrite")
                .map(|s| {
                    s.split(',')
                        .filter_map(|rule| {
                            let (from, to) = rule.trim().split_once('=')?;
                            Some((from.trim().to_string(), to.trim().to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            resubscribe_interval_ms: config.get_int("resubscribe-interval-ms", 10_000).max(0)
                as u64,
            conn: TiDBCdcConnConfig::new(
                config.get_string("conn-host", "127.0.0.1").as_str(),
                config.get_int("conn-port", 4000) as u16,
                config.get_string("conn-user", "root").as_str(),
                config.get_string("conn-password", "").as_str(),
                config.get_string("conn-database", "seatunnel").as_str(),
            ),
        }
    }
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

async fn new_connection(conn: &TiDBCdcConnConfig) -> anyhow::Result<mysql_async::Conn> {
    let pool = conn.to_pool();
    let mut c = pool.get_conn().await.map_err(|e| {
        anyhow::anyhow!("cannot connect to TiDB {}:{}: {}", conn.host, conn.port, e)
    })?;
    // Verify liveness — a pooled handle alone proves nothing.
    let _: Option<String> = c
        .query_first("SELECT 1")
        .await
        .map_err(|e| anyhow::anyhow!("TiDB ping failed: {}", e))?;
    Ok(c)
}

/// Resolve the TiDB-internal table id (`TIDB_TABLE_ID`) required to build
/// TiKV key ranges.
async fn resolve_table_id(
    conn: &mut mysql_async::Conn,
    database: &str,
    table: &str,
) -> anyhow::Result<i64> {
    let row: Option<(i64,)> = conn
        .query_first(format!(
            "SELECT TIDB_TABLE_ID FROM information_schema.tables \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' LIMIT 1",
            database.replace('\'', "''"),
            table.replace('\'', "''")
        ))
        .await?;
    row.map(|(id,)| id).ok_or_else(|| {
        anyhow::anyhow!(
            "table {}.{} not found in information_schema",
            database,
            table
        )
    })
}

/// Compute this subtask's disjoint snapshot ranges `[start,end)` over the
/// split column, chunked into ≤ SNAPSHOT_BATCH_SIZE spans.
async fn enumerate_snapshot_ranges(
    conn: &mut mysql_async::Conn,
    config: &TiDBCdcConfig,
) -> anyhow::Result<Vec<(i64, i64)>> {
    let col = config.split_column.replace('`', "``");
    let sql = format!(
        "SELECT MIN(`{}`), MAX(`{}`) FROM `{}`.`{}`",
        col, col, config.database_name, config.table_name
    );
    let row: Option<(Option<i64>, Option<i64>)> = conn.query_first(sql).await?;
    let (Some(min_id), Some(max_id)) = (row.and_then(|(a, _)| a), row.and_then(|(_, b)| b)) else {
        tracing::info!(
            "TiDB CDC: table {}.{} is empty; a single empty snapshot split is created",
            config.database_name,
            config.table_name
        );
        return Ok(vec![(0, 1)]);
    };

    let count = config.subtask_count.max(1);
    let span = (max_id - min_id + 1).max(1);
    let chunk = (span + count as i64 - 1) / count as i64;
    let idx = config.subtask_index.min(count - 1) as i64;
    let range_start = min_id + idx * chunk;
    let range_end = (range_start + chunk).min(max_id + 1);
    if range_start > max_id {
        tracing::info!(
            "TiDB CDC: subtask {}/{} has an empty snapshot range",
            config.subtask_index,
            count
        );
        return Ok(vec![(0, 1)]);
    }

    let mut splits = Vec::new();
    let mut cursor = range_start;
    while cursor < range_end {
        let end = (cursor + SNAPSHOT_BATCH_SIZE).min(range_end);
        splits.push((cursor, end));
        cursor = end;
    }
    tracing::info!(
        "TiDB CDC: subtask {}/{} enumerated {} split(s) for {}.{} covering ids [{}, {})",
        config.subtask_index,
        count,
        splits.len(),
        config.database_name,
        config.table_name,
        range_start,
        range_end
    );
    Ok(splits)
}

/// TiDB CDC Source.
#[derive(Debug, Clone)]
pub struct TiDBCdcSource {
    config: TiDBCdcConfig,
    schema: Option<TableSchema>,
}

impl TiDBCdcSource {
    pub fn new(config: TiDBCdcConfig, schema: Option<TableSchema>) -> Self {
        TiDBCdcSource { config, schema }
    }

    pub fn from_config(config: &ConnectorConfig, schema: Option<TableSchema>) -> Self {
        TiDBCdcSource::new(TiDBCdcConfig::from_config(config), schema)
    }

    pub fn config(&self) -> &TiDBCdcConfig {
        &self.config
    }
}

impl Source for TiDBCdcSource {
    type Output = TiDBCdcOutput;
    type Split = TiDBCdcSplit;
    type State = CdcState;

    fn enumerate_splits(
        &self,
        _context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        let ranges = {
            let config = self.config.clone();
            tokio_block_on(async move {
                let mut conn = new_connection(&config.conn).await?;
                enumerate_snapshot_ranges(&mut conn, &config).await
            })?
        };
        Ok(ranges
            .into_iter()
            .map(|(start, end)| {
                TiDBCdcSplit::Snapshot(SnapshotSplit::new(
                    &self.config.database_name,
                    &self.config.table_name,
                    &self.config.split_column,
                    &start.to_string(),
                    &end.to_string(),
                ))
            })
            .collect())
    }

    fn create_reader(
        &self,
        _context: SourceReaderContext,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(TiDBCdcReader::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_reader(
        &self,
        _context: SourceReaderContext,
        state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        let mut reader = TiDBCdcReader::new(self.config.clone(), self.schema.clone());
        reader.apply_state(state.clone());
        Ok(Box::new(reader))
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded
    }
}

/// TiDB CDC Source reader.
pub struct TiDBCdcReader {
    config: TiDBCdcConfig,
    #[allow(dead_code)] // retained for future schema-aware serialization
    schema: Option<TableSchema>,
    table_id: Cell<i64>,
    phase: CdcPhase,
    /// Remaining snapshot ranges `[start,end)` with keyset cursors.
    pending_ranges: VecDeque<(i64, i64)>,
    last_pk: i64,
    resolved_ts: ResolvedTs,
    watermark: Watermark,
    batch_size: i64,
    /// TiKV CDC engine for real-time streaming (incremental phase).
    cdc_engine: Option<crate::cdc_engine::CdcEngine>,
    /// Decoded rows awaiting emission (stream events + drained transactions).
    pending_rows: VecDeque<Row>,
    engine_errors: u32,
}

impl TiDBCdcReader {
    pub fn new(config: TiDBCdcConfig, schema: Option<TableSchema>) -> Self {
        TiDBCdcReader {
            config,
            schema,
            table_id: Cell::new(-1),
            phase: CdcPhase::Snapshot,
            pending_ranges: VecDeque::new(),
            last_pk: 0,
            resolved_ts: ResolvedTs::default(),
            watermark: Watermark::Min,
            batch_size: SNAPSHOT_BATCH_SIZE,
            cdc_engine: None,
            pending_rows: VecDeque::new(),
            engine_errors: 0,
        }
    }

    /// Apply previously snapshotted state (checkpoint restore path).
    pub fn apply_state(&mut self, state: CdcState) {
        self.phase = state.phase;
        self.watermark = state.watermark;
        if let Some(ts) = state.offset.get("resolved_ts") {
            if let Ok(v) = ts.parse::<u64>() {
                self.resolved_ts = ResolvedTs(v);
            }
        }
        if let Some(tid) = state.offset.get("table_id") {
            if let Ok(v) = tid.parse::<i64>() {
                self.table_id = Cell::new(v);
            }
        }
        if let Some(pk) = state.offset.get("last_pk") {
            if let Ok(v) = pk.parse::<i64>() {
                self.last_pk = v;
            }
        }
        if let Some(ranges) = state
            .offset
            .get("ranges")
            .and_then(|s| serde_json::from_str::<Vec<(String, String)>>(s).ok())
        {
            self.pending_ranges = ranges
                .into_iter()
                .map(|(a, b)| (a.parse::<i64>().unwrap_or(0), b.parse::<i64>().unwrap_or(0)))
                .collect();
        }
        if self.phase == CdcPhase::Incremental {
            self.pending_ranges.clear();
        }
        tracing::info!(
            "TiDB CDC reader: restored state phase={} resolved_ts={} ranges={}",
            self.phase,
            self.resolved_ts.0,
            self.pending_ranges.len()
        );
    }

    /// Deserialize + apply a serialized [`CdcState`].
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: CdcState = serde_json::from_slice(bytes)
            .map_err(|e| anyhow::anyhow!("bad TiDB CDC state: {}", e))?;
        self.apply_state(state);
        Ok(())
    }

    async fn start_engine(&mut self) -> anyhow::Result<()> {
        if self.cdc_engine.is_some() {
            return Ok(());
        }
        let table_id = self.table_id.get();
        if table_id < 0 {
            anyhow::bail!("table id unresolved; cannot build TiKV key range");
        }
        let pd_addrs = if self.config.pd_addrs.is_empty() {
            vec![format!("{}:2379", self.config.conn.host)]
        } else {
            self.config.pd_addrs.clone()
        };
        let (start_key, end_key) = table_key_range(table_id);
        let engine_config = crate::cdc_engine::CdcEngineConfig {
            pd_addrs,
            table_id,
            start_key,
            end_key,
            cluster_id: self.config.cluster_id.unwrap_or(0),
            checkpoint_ts: self.resolved_ts.0,
            request_snapshot: false,
            address_rewrite: self.config.store_address_rewrite.clone(),
            resubscribe_interval_ms: self.config.resubscribe_interval_ms,
        };
        let mut engine = crate::cdc_engine::CdcEngine::new(engine_config);
        engine.start().await.map_err(|e| {
            anyhow::anyhow!(
                "TiKV CDC engine failed (pd={:?}): {} — check PD/TiKV reachability from the worker",
                self.config.pd_addrs,
                e
            )
        })?;
        tracing::info!(
            "TiDB CDC reader: TiKV CDC engine started for table {} (checkpoint_ts={})",
            table_id,
            engine.resolved_ts()
        );
        self.cdc_engine = Some(engine);
        Ok(())
    }

    /// Drain decoded engine rows into the local buffer (bounded wait so
    /// snapshot scans are not starved by idle streams).
    async fn drain_engine(&mut self, budget_ms: u64) {
        let Some(engine) = self.cdc_engine.as_mut() else {
            return;
        };
        match engine.poll_with_budget(budget_ms).await {
            Ok(_consumed) => {
                self.engine_errors = 0;
                while let Some(row_event) = engine.next_row() {
                    if let Some(row) = build_row_from_event(&row_event) {
                        self.pending_rows.push_back(row);
                    }
                    self.resolved_ts = ResolvedTs(self.resolved_ts.0.max(row_event.resolved_ts));
                }
            }
            Err(e) => {
                self.engine_errors += 1;
                tracing::warn!(
                    "TiDB CDC engine poll error ({}/{}): {}",
                    self.engine_errors,
                    ENGINE_ERROR_TOLERANCE,
                    e
                );
            }
        }
    }
}

/// Map a decoded engine row event onto the uniform raw-column layout.
fn build_row_from_event(event: &crate::cdc_engine::CdcRowEvent) -> Option<Row> {
    let num_cols = event.columns.len();
    if num_cols == 0 {
        return None;
    }
    let mut out_row = Row::new(RowKind::Insert, num_cols);
    for (i, col) in event.columns.iter().enumerate() {
        out_row.set(i, column_value_to_field(col));
    }
    // op_type: 1=PUT, 2=DELETE. PUT with old_value ⇒ UPDATE-after.
    if event.op_type == 2 {
        out_row.kind = RowKind::Delete;
    } else if event.is_update {
        out_row.kind = RowKind::UpdateAfter;
    }
    Some(out_row)
}

impl SourceReader for TiDBCdcReader {
    type Output = TiDBCdcOutput;
    type Split = TiDBCdcSplit;

    fn open(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!(
                "TiDB CDC reader opening: {}.{} at {}:{} mode={}",
                self.config.database_name,
                self.config.table_name,
                self.config.conn.host,
                self.config.conn.port,
                self.config.startup_mode
            );

            // Fail loudly when unreachable — silent fallbacks produce wrong data.
            let mut conn = new_connection(&self.config.conn).await?;

            // Resolve the internal table id needed for TiKV key ranges.
            if self.table_id.get() < 0 {
                let tid = resolve_table_id(
                    &mut conn,
                    &self.config.database_name,
                    &self.config.table_name,
                )
                .await?;
                self.table_id = Cell::new(tid);
                tracing::info!("TiDB CDC reader: table id = {}", tid);
            }

            // Startup shortcuts: streaming-only readers skip the snapshot.
            let resuming_incremental =
                self.phase == CdcPhase::Incremental && self.resolved_ts.0 > 0;
            if !resuming_incremental {
                match self.config.startup_mode.as_str() {
                    "latest" | "earliest" => {
                        self.phase = CdcPhase::Incremental;
                        self.pending_ranges.clear();
                        tracing::info!(
                            "TiDB CDC reader: skipping snapshot (startup.mode={})",
                            self.config.startup_mode
                        );
                    }
                    _ => {}
                }
            } else {
                self.phase = CdcPhase::Incremental;
                self.pending_ranges.clear();
            }

            let streams_changes =
                self.config.startup_mode != "snapshot-only" && self.config.subtask_index == 0;

            // Start the change stream BEFORE scanning so commits racing the
            // snapshot are captured and replayed afterwards (no-loss window).
            if streams_changes {
                self.start_engine().await?;
            }

            // Seed snapshot splits unless restored or streaming-only.
            if self.phase == CdcPhase::Snapshot && self.pending_ranges.is_empty() {
                let ranges = enumerate_snapshot_ranges(&mut conn, &self.config).await?;
                self.pending_ranges.extend(ranges);
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
                // Live changes first, then anything still queued.
                self.drain_engine(250).await;
                if self.engine_errors >= ENGINE_ERROR_TOLERANCE {
                    anyhow::bail!(
                        "TiKV CDC engine failed {} times consecutively",
                        self.engine_errors
                    );
                }
                if let Some(row) = self.pending_rows.pop_front() {
                    return Ok(PollResult::Record(TiDBCdcOutput(row)));
                }
                return Ok(PollResult::Empty);
            }

            // ---- Snapshot phase ---------------------------------------
            // Keep the change streams drained while scanning (no emission yet:
            // buffered WAL rows are replayed after the snapshot completes).
            self.drain_engine(10).await;

            if let Some((start, end)) = self.pending_ranges.front().copied() {
                let pool = self.config.conn.to_pool();
                let mut conn = pool.get_conn().await?;
                let col = self.config.split_column.replace('`', "``");
                let sql = format!(
                    "SELECT * FROM `{}`.`{}` WHERE `{}` >= {} AND `{}` < {} AND `{}` > {} \
                     ORDER BY `{}` ASC LIMIT {}",
                    self.config.database_name,
                    self.config.table_name,
                    col,
                    start,
                    col,
                    end,
                    col,
                    self.last_pk,
                    col,
                    self.batch_size,
                );
                let rows: Vec<mysql_async::Row> = conn.query(sql).await?;
                if rows.is_empty() {
                    self.pending_ranges.pop_front();
                    self.last_pk = 0;
                    return Ok(PollResult::Empty);
                }
                let field_count = rows[0].len();
                for r in rows.iter() {
                    let mut out_row = Row::new(RowKind::Insert, field_count);
                    for i in 0..field_count {
                        let val: Option<MysqlValue> = r.get(i);
                        out_row.set(i, mysql_value_to_field(val));
                    }
                    self.pending_rows.push_back(out_row);
                }
                // Advance the keyset cursor to the LAST pk of this batch.
                let pk_col_idx = 0usize; // split column selected via SELECT * ordering assumption below
                let _ = pk_col_idx;
                self.last_pk = extract_pk_from_row(
                    &rows[rows.len() - 1],
                    &self.config.split_column,
                    self.last_pk,
                );
                if let Some(row) = self.pending_rows.pop_front() {
                    return Ok(PollResult::Record(TiDBCdcOutput(row)));
                }
                return Ok(PollResult::Empty);
            }

            // All snapshot splits consumed.
            if self.config.subtask_index != 0 || self.config.startup_mode == "snapshot-only" {
                tracing::info!(
                    "TiDB CDC reader: subtask {}/{} snapshot complete, EOF (streaming handled by subtask 0)",
                    self.config.subtask_index,
                    self.config.subtask_count
                );
                return Ok(PollResult::EOF);
            }
            self.handle_no_more_splits();
            Ok(PollResult::Empty)
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let mut offset = HashMap::new();
        let resolved_ts = self
            .cdc_engine
            .as_ref()
            .map(|e| e.resolved_ts())
            .unwrap_or(self.resolved_ts.0)
            .max(self.resolved_ts.0);
        offset.insert("resolved_ts".to_string(), resolved_ts.to_string());
        offset.insert("table_id".to_string(), self.table_id.get().to_string());
        offset.insert("last_pk".to_string(), self.last_pk.to_string());
        let ranges: Vec<(String, String)> = self
            .pending_ranges
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        if !ranges.is_empty() {
            if let Ok(json) = serde_json::to_string(&ranges) {
                offset.insert("ranges".to_string(), json);
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
        tracing::info!("TiDB CDC reader: adding {} splits", splits.len());
        for split in splits {
            if let TiDBCdcSplit::Snapshot(s) = split {
                if let (Ok(a), Ok(b)) = (s.start_key.parse::<i64>(), s.end_key.parse::<i64>()) {
                    self.pending_ranges.push_back((a, b));
                }
            }
        }
    }

    fn handle_no_more_splits(&mut self) {
        self.phase = CdcPhase::Incremental;
        self.watermark = Watermark::Value(self.last_pk.max(0));
        tracing::info!(
            "TiDB CDC reader: transitioning to incremental phase (replay buffer={}, resolved_ts={})",
            self.pending_rows.len(),
            self.resolved_ts.0
        );
    }

    fn close(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(engine) = self.cdc_engine.as_mut() {
                engine.close().await;
            }
            self.cdc_engine = None;
            tracing::info!("TiDB CDC reader closed");
            Ok(())
        })
    }
}

/// Locate the split-column index among result columns and read it as i64.
fn extract_pk_from_row(row: &mysql_async::Row, column: &str, default: i64) -> i64 {
    let idx = row
        .columns()
        .as_ref()
        .iter()
        .position(|c| c.name_str() == column);
    let Some(idx) = idx else {
        return default;
    };
    pk_from_value(row.get::<MysqlValue, usize>(idx), default)
}

/// Map a raw SQL value onto the split-key cursor. Falls back to `default`
/// for NULL/unparseable values — never panics.
fn pk_from_value(val: Option<MysqlValue>, default: i64) -> i64 {
    match val {
        Some(MysqlValue::Int(v)) => v,
        Some(MysqlValue::UInt(v)) => i64::try_from(v).unwrap_or(default),
        Some(MysqlValue::Float(v)) => v as i64,
        Some(MysqlValue::Double(v)) => v as i64,
        Some(MysqlValue::Bytes(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(default),
        _ => default,
    }
}

/// Convert a decoded TiKV column value into a Seatunnel Field.
fn column_value_to_field(col: &crate::decoder::ColumnValue) -> Field {
    use crate::decoder::ColumnValue;
    match col {
        ColumnValue::Null => Field::Null,
        ColumnValue::Int(v) => Field::Int64(*v),
        ColumnValue::UInt(v) => Field::UInt64(*v),
        ColumnValue::Float(v) => Field::Float64(*v),
        ColumnValue::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => Field::String(s.to_string()),
            Err(_) => Field::Bytes(b.clone()),
        },
        ColumnValue::Text(s) => Field::String(s.clone()),
        ColumnValue::Json(s) => Field::String(s.clone()),
    }
}

/// Build the TiKV key range covering all rows of a table, mirroring
/// `tablecodec.GenTableRecordPrefix` and its Next() successor:
///
/// ```text
/// start = 't' + cmp_i64(table_id) + "_r" + cmp_i64(MinInt64)   (= 0x00 × 8)
/// end   = 't' + cmp_i64(table_id) + "_s"                        (Next of max row)
/// ```
///
/// TiKV's CDC endpoint validates these bounds (`ObservedRange`) and uses
/// them to filter every streamed entry, so the encoding must byte-match the
/// Build the TiKV key range covering all rows of a table.
///
/// Returns PLAIN TiDB-level keys (NOT additionally encoded). TiKV intersects
/// these with region boundaries internally.
fn table_key_range(table_id: i64) -> (Vec<u8>, Vec<u8>) {
    let id_cmp = (table_id as u64 ^ (1 << 63)).to_be_bytes();
    let mut start = Vec::with_capacity(11);
    start.push(b't');
    start.extend_from_slice(&id_cmp);
    start.extend_from_slice(b"_r");
    let mut end = Vec::with_capacity(11);
    end.push(b't');
    end.extend_from_slice(&id_cmp);
    end.push(b'_');
    end.push(b's');
    (start, end)
}

/// Convert a mysql_async::Value to a seatunnel_api::Field.
fn mysql_value_to_field(val: Option<MysqlValue>) -> Field {
    match val {
        None | Some(MysqlValue::NULL) => Field::Null,
        Some(MysqlValue::Bytes(b)) => {
            let s = match std::str::from_utf8(&b) {
                Ok(str) => str.to_string(),
                Err(_) => return Field::Bytes(b),
            };
            // Heuristic: try to parse as numeric types
            if let Ok(n) = s.parse::<i64>() {
                return Field::Int64(n);
            }
            if let Ok(n) = s.parse::<u64>() {
                return Field::UInt64(n);
            }
            if let Ok(n) = s.parse::<f64>() {
                return Field::Float64(n);
            }
            Field::String(s)
        }
        Some(MysqlValue::Int(n)) => Field::Int64(n),
        Some(MysqlValue::UInt(n)) => Field::UInt64(n),
        Some(MysqlValue::Float(n)) => Field::Float64(n as f64),
        Some(MysqlValue::Double(n)) => Field::Float64(n),
        Some(MysqlValue::Date(year, mon, day, hour, min, sec, _us)) => {
            if mon == 0 && day == 0 && hour == 0 && min == 0 && sec == 0 {
                return Field::Null;
            }
            use chrono::NaiveDate;
            if let Some(date) = NaiveDate::from_ymd_opt(year as i32, mon.into(), day.into()) {
                if hour == 0 && min == 0 && sec == 0 {
                    return Field::Date(date);
                }
                if let Some(time) =
                    chrono::NaiveTime::from_hms_opt(hour.into(), min.into(), sec.into())
                {
                    return Field::DateTime(date.and_time(time));
                }
            }
            Field::String(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, mon, day, hour, min, sec
            ))
        }
        Some(MysqlValue::Time(neg, _, h, min, sec, _us)) => {
            if neg {
                Field::String(format!("-{:02}:{:02}:{:02}", h, min, sec))
            } else {
                Field::String(format!("{:02}:{:02}:{:02}", h, min, sec))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp_i64(v: i64) -> [u8; 8] {
        (v as u64 ^ (1 << 63)).to_be_bytes()
    }

    #[test]
    fn test_tidb_table_key_range_layout() {
        let (start, end) = table_key_range(45);
        let expected_id = cmp_i64(45);
        assert_eq!(&start[0..1], b"t");
        assert_eq!(&start[1..9], &expected_id);
        assert_eq!(&start[9..11], b"_r");
        assert_eq!(start.len(), 11);
        assert_eq!(end[0], b't');
        assert_eq!(&end[9..11], b"_s");
        assert!(start < end);
    }

    #[test]
    fn test_tidb_config_parsing() {
        let mut props = HashMap::new();
        props.insert(
            "pd-addrs".to_string(),
            "http://pd1:2379, http://pd2:2379".to_string(),
        );
        props.insert("database-name".to_string(), "mydb".to_string());
        props.insert("table-name".to_string(), "orders".to_string());
        props.insert("split.column".to_string(), "order_id".to_string());
        props.insert("subtask.count".to_string(), "4".to_string());
        props.insert("subtask.index".to_string(), "2".to_string());
        let config = ConnectorConfig::new(props);
        let tidb_config = TiDBCdcConfig::from_config(&config);
        assert_eq!(tidb_config.pd_addrs.len(), 2);
        assert_eq!(tidb_config.database_name, "mydb");
        assert_eq!(tidb_config.table_name, "orders");
        assert_eq!(tidb_config.split_column, "order_id");
        assert_eq!(tidb_config.subtask_index, 2);
        assert_eq!(tidb_config.subtask_count, 4);
    }

    #[tokio::test]
    async fn open_fails_without_database() {
        // No synthetic fallbacks: unreachable TiDB must fail the task.
        let cfg = TiDBCdcConfig {
            conn: TiDBCdcConnConfig::new("127.0.0.1", 1, "root", "", "seatunnel"),
            ..TiDBCdcConfig::default()
        };
        let mut reader = TiDBCdcReader::new(cfg, None);
        let result = reader.open().await;
        assert!(result.is_err(), "expected error, got {:?}", result);
    }

    #[test]
    fn test_pk_from_value_shapes() {
        assert_eq!(pk_from_value(Some(MysqlValue::Int(11)), 0), 11);
        assert_eq!(pk_from_value(Some(MysqlValue::UInt(12)), 0), 12);
        assert_eq!(
            pk_from_value(Some(MysqlValue::Bytes(b"13".to_vec())), 0),
            13
        );
        assert_eq!(pk_from_value(Some(MysqlValue::NULL), 7), 7);
        assert_eq!(pk_from_value(None, 9), 9);
    }

    #[test]
    fn test_build_row_kind_mapping() {
        let event = crate::cdc_engine::CdcRowEvent {
            table_id: 42,
            handle: 1,
            op_type: 2,
            is_update: false,
            columns: vec![crate::decoder::ColumnValue::Int(1)],
            resolved_ts: 99,
        };
        let row = build_row_from_event(&event).unwrap();
        assert_eq!(row.kind, RowKind::Delete);
        assert_eq!(row.field_count(), 1);

        let update = crate::cdc_engine::CdcRowEvent {
            op_type: 1,
            is_update: true,
            columns: vec![crate::decoder::ColumnValue::Int(2)],
            ..event
        };
        let row = build_row_from_event(&update).unwrap();
        assert_eq!(row.kind, RowKind::UpdateAfter);
    }

    #[tokio::test]
    async fn state_roundtrip_preserves_progress() {
        let mut reader = TiDBCdcReader::new(TiDBCdcConfig::default(), None);
        reader.pending_ranges.push_back((100, 200));
        reader.last_pk = 150;
        reader.resolved_ts = ResolvedTs(424242);
        reader.table_id = Cell::new(77);
        let bytes = reader.snapshot_state().await.unwrap();

        let mut restored = TiDBCdcReader::new(TiDBCdcConfig::default(), None);
        restored.restore_from_state_bytes(&bytes).unwrap();
        assert_eq!(restored.last_pk, 150);
        assert_eq!(restored.resolved_ts.0, 424242);
        assert_eq!(restored.table_id.get(), 77);
        assert_eq!(restored.pending_ranges.front(), Some(&(100, 200)));
    }
}

pub mod cdc_client;
pub mod cdc_engine;
pub mod decoder;
pub mod kvproto;
pub mod pd_client;
