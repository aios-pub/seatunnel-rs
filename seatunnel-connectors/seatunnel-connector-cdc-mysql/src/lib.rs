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
    alter_table_target, build_table_selector, CdcConfig, CdcPhase, CdcSource, CdcState,
    IncrementalSplit, SchemaEvolutionConfig, SchemaWatcher, SnapshotSplit, TableSelector,
    Watermark,
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
/// Events drained per poll during the timestamp warm-up skip.
const TIMESTAMP_WARMUP_DRAIN_BUDGET: usize = 2000;

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


/// Stop condition for bounded capture (official `stop.mode`).
#[derive(Debug, Clone, PartialEq)]
pub enum MySqlStopMode {
    /// Run until cancelled (default).
    Never,
    /// Stop after reaching the position captured at job start.
    Latest,
    /// Stop at an explicit binlog position.
    Specific { file: String, position: u64 },
    /// Stop at an explicit wall-clock time (ms).
    Timestamp { timestamp: i64 },
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
    /// Upper bound when `server-id` is given as a range ("5400-5408").
    pub server_id_range: u32,
    /// Split key column (defaults to `id`).
    pub split_column: String,
    /// This reader's subtask index / total subtask count — snapshot ranges
    /// are partitioned so each subtask scans a disjoint id interval.
    pub subtask_index: usize,
    pub subtask_count: usize,
    /// TCP connect timeout (official `connect.timeout.ms`, default 30 s).
    pub connect_timeout_ms: u64,
    /// Connection retries before failing (official `connect.max-retries`).
    pub connect_max_retries: u32,
    /// Pool max connections (official `connection.pool.size`, default 20).
    pub connection_pool_size: u32,
    /// Rows per snapshot split (official `snapshot.split.size`, 8096).
    pub snapshot_split_size: i64,
    /// Snapshot page size (official `snapshot.fetch.size`, 1024).
    pub snapshot_fetch_size: i64,
    /// Stop condition (official `stop.mode` + `stop.*` options).
    pub stop_mode: MySqlStopMode,
    /// Resolved database/table selection.
    pub table_selector: TableSelector,
    /// Warnings for official-but-unimplemented options (logged at open).
    pub compat_warnings: Vec<String>,
    /// Schema-evolution settings (DDL-to-schema-change-event pipeline).
    pub schema_evolution: SchemaEvolutionConfig,
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
            server_id_range: 0,
            split_column: "id".to_string(),
            subtask_index: 0,
            subtask_count: 1,
            schema_evolution: SchemaEvolutionConfig::default(),
            connect_timeout_ms: 30_000,
            connect_max_retries: 3,
            connection_pool_size: 20,
            snapshot_split_size: 8096,
            snapshot_fetch_size: 1024,
            stop_mode: MySqlStopMode::Never,
            table_selector: TableSelector::from_legacy("seatunnel", "users"),
            compat_warnings: Vec::new(),
        }
    }
}

impl MySqlCdcConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        // `url` (jdbc:mysql://host:port/db) is the official connection
        // option; hostname/port stay as simpler alternatives.
        let (url_host, url_port, url_db) = config
            .get("url")
            .and_then(|u| parse_mysql_jdbc_url(u))
            .unwrap_or_default();
        let database_name = {
            let v = config.get_string("database-name", &url_db);
            if v.is_empty() { "seatunnel".to_string() } else { v }
        };
        let table_name = config.get_string("table-name", "users");
        MySqlCdcConfig {
            hostname: {
                let v = config.get_string("hostname", &url_host);
                if v.is_empty() { "localhost".to_string() } else { v }
            },
            port: {
                let p = config.get_int("port", -1);
                if p > 0 { p as u16 } else if url_port > 0 { url_port } else { 3306 }
            },
            username: config.get_string("username", "root"),
            password: config.get_string("password", ""),
            database_name: database_name.clone(),
            table_name: table_name.clone(),
            parallelism: config.get_int("parallelism", 4) as usize,
            startup_mode: config
                .get("startup.mode")
                .map(|s| match s.as_str() {
                    "initial" => MySqlStartupMode::Initial,
                    "snapshot" | "snapshot-only" => MySqlStartupMode::SnapshotOnly,
                    "earliest" => MySqlStartupMode::Earliest,
                    "latest" => MySqlStartupMode::Latest,
                    "timestamp" => MySqlStartupMode::Timestamp {
                        timestamp: config.get_int(
                            "startup.timestamp",
                            config.get_int("startup_timestamp", 0),
                        ),
                    },
                    "specific" | "specific-offset" => MySqlStartupMode::Specific {
                        // Official keys: startup.specific-offset.file/.pos
                        // (+ .gtid-set); the shorter startup.specific.*
                        // forms remain as aliases.
                        file: config.get_string(
                            "startup.specific-offset.file",
                            &config.get_string("startup.specific.file", ""),
                        ),
                        position: config
                            .get_int(
                                "startup.specific-offset.pos",
                                config.get_int(
                                    "startup.specific.pos",
                                    config.get_int("startup.specific.position", 0),
                                ),
                            )
                            .max(0) as u64,
                        gtid_set: {
                            let gtid = config.get_string(
                                "startup.specific-offset.gtid-set",
                                &config.get_string(
                                    "startup.specific.gtid-set",
                                    &config.get_string("startup.specific.gtid_set", ""),
                                ),
                            );
                            if gtid.is_empty() { None } else { Some(gtid) }
                        },
                    },
                    _ => MySqlStartupMode::Initial,
                })
                .unwrap_or(MySqlStartupMode::Initial),
            server_timezone: config.get_string("server-timezone", "+00:00"),
            server_id: {
                let raw = config.get_string("server-id", "0");
                if let Some((lo, _)) = raw.split_once('-') {
                    lo.trim().parse::<u32>().unwrap_or(0)
                } else {
                    raw.trim().parse::<u32>().unwrap_or(0)
                }
            },
            server_id_range: {
                let raw = config.get_string("server-id", "");
                if let Some((_, hi)) = raw.split_once('-') {
                    let lo: u32 = raw.split('-').next().unwrap_or("0").trim().parse().unwrap_or(0);
                    let hi: u32 = hi.trim().parse().unwrap_or(lo);
                    hi.saturating_sub(lo)
                } else {
                    0
                }
            },
            split_column: config.get_string("split.column", "id"),
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
            schema_evolution: SchemaEvolutionConfig::from_config(config),
            connect_timeout_ms: config
                .get_int("connect.timeout.ms", config.get_int("connect.timeout-ms", 30_000))
                .max(100) as u64,
            connect_max_retries: config
                .get_int("connect.max-retries", config.get_int("connect.max_retries", 3))
                .max(0) as u32,
            connection_pool_size: config
                .get_int(
                    "connection.pool.size",
                    config.get_int("connection-pool-size", 20),
                )
                .max(1) as u32,
            snapshot_split_size: config
                .get_int("snapshot.split.size", config.get_int("snapshot.split_size", 8096))
                .max(1),
            snapshot_fetch_size: config
                .get_int("snapshot.fetch.size", config.get_int("snapshot_fetch_size", 1024))
                .max(1),
            stop_mode: parse_stop_mode(config),
            table_selector: build_table_selector(config, &database_name, &table_name),
            compat_warnings: seatunnel_connector_cdc_base::compatibility_warnings(config),
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
        if self.server_id != 0 && self.server_id_range == 0 {
            return self.server_id;
        }
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        if self.server_id != 0 && self.server_id_range > 0 {
            // Explicit `server-id: "5400-5408"` range: assign unique ids
            // inside the range (official cluster convention).
            return self.server_id + (n % self.server_id_range);
        }
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

}


/// Parse `stop.mode` + `stop.*` options (official: never | latest |
/// specific; `timestamp` additionally accepted for symmetry with Java).
fn parse_stop_mode(config: &ConnectorConfig) -> MySqlStopMode {
    match config.get_string("stop.mode", "never").to_lowercase().as_str() {
        "latest" => MySqlStopMode::Latest,
        "specific" | "specific-offset" => MySqlStopMode::Specific {
            file: config.get_string("stop.specific-offset.file", ""),
            position: config
                .get_int(
                    "stop.specific-offset.pos",
                    config.get_int("stop.specific.pos", 0),
                )
                .max(0) as u64,
        },
        "timestamp" => MySqlStopMode::Timestamp {
            timestamp: config.get_int("stop.timestamp", 0),
        },
        _ => MySqlStopMode::Never,
    }
}

/// Parse `jdbc:mysql://host:port/db?...` (or `mysql://`) into
/// (host, port, database). Returns `None` when the value is not a URL.
fn parse_mysql_jdbc_url(url: &str) -> Option<(String, u16, String)> {
    let rest = url.strip_prefix("jdbc:mysql://").or_else(|| url.strip_prefix("mysql://"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let database = path.split('?').next().unwrap_or("").to_string();
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(3306)),
        None => (authority.to_string(), 3306),
    };
    if host.is_empty() {
        return None;
    }
    Some((host, port, database))
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
        let constraints = mysql_async::PoolConstraints::new(
            1,
            config.connection_pool_size.max(1) as usize,
        )
        .expect("valid pool constraints");
        let opts = OptsBuilder::default()
            .ip_or_hostname(&config.hostname)
            .tcp_port(config.port)
            .user(Some(&config.username))
            .pass(Some(&config.password))
            .db_name(Some(&config.database_name))
            .pool_opts(mysql_async::PoolOpts::new().with_constraints(constraints));
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

    // Resolve every table matched by the selection (exact names, patterns,
    // legacy wildcards) via information_schema.
    let tables = resolve_captured_tables(&mut conn, config).await?;
    if tables.is_empty() {
        anyhow::bail!(
            "MySQL CDC: no tables matched the configured selection (database-name: {:?}, table-name: {:?}, selector databases: {:?})",
            config.database_name,
            config.table_name,
            config.table_selector.databases()
        );
    }

    let parallelism = config.subtask_count.max(1);
    let idx = (config.subtask_index.min(parallelism - 1)) as i64;
    let mut splits = Vec::new();

    for (db, table) in &tables {
        let table_ref = format!("`{}`.`{}`", db, table);
        let split_column = config
            .table_selector
            .split_column_for(db, table)
            .map(str::to_string)
            .unwrap_or_else(|| config.split_column.clone());

        let min_max: Option<(Option<i64>, Option<i64>)> = conn
            .query_first(format!(
                "SELECT MIN(`{}`), MAX(`{}`) FROM {}",
                split_column, split_column, table_ref
            ))
            .await?;
        let (Some(min_id), Some(max_id)) =
            (min_max.and_then(|(a, _)| a), min_max.and_then(|(_, b)| b))
        else {
            tracing::info!("MySQL CDC: table {} is empty; skipped", table_ref);
            continue;
        };

        // Rows-per-chunk: honor snapshot.split.size, but never larger than
        // the per-subtask span so parallel readers keep disjoint ranges.
        let span = (max_id - min_id + 1).max(1);
        let by_parallelism = (span + parallelism as i64 - 1) / parallelism as i64;
        let chunk = by_parallelism.min(config.snapshot_split_size.max(1)).max(1);

        // Round-robin chunk→subtask assignment: subtask i owns chunks
        // i, i+parallelism, ... of the id space.
        let mut chunk_start = min_id;
        let mut chunk_idx = 0i64;
        let mut owned = 0i64;
        while chunk_start <= max_id {
            let chunk_end = chunk_start.saturating_add(chunk).min(max_id + 1);
            if chunk_idx % parallelism as i64 == idx {
                splits.push(SnapshotSplit::new(
                    db,
                    table,
                    &split_column,
                    &chunk_start.to_string(),
                    &chunk_end.to_string(),
                ));
                owned += 1;
            }
            chunk_start = chunk_end;
            chunk_idx += 1;
        }
        tracing::info!(
            "MySQL CDC: subtask {}/{} owns {} chunk(s) of {} [ids {}..{}] (chunk size {})",
            config.subtask_index,
            parallelism,
            owned,
            table_ref,
            min_id,
            max_id,
            chunk
        );
    }
    Ok(splits)
}

/// Resolve concrete `(database, table)` pairs for the configured selection.
async fn resolve_captured_tables(
    conn: &mut mysql_async::Conn,
    config: &MySqlCdcConfig,
) -> anyhow::Result<Vec<(String, String)>> {
    let rows: Vec<mysql_async::Row> = conn
        .query("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE'")
        .await?;
    let mut matched = Vec::new();
    for row in rows {
        let schema: Option<String> = row.get(0).unwrap_or(None);
        let table: Option<String> = row.get(1).unwrap_or(None);
        let (Some(schema), Some(table)) = (schema, table) else {
            continue;
        };
        if config.table_selector.matches(&schema, &table) {
            matched.push((schema, table));
        }
    }
    Ok(matched)
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
    /// Schema-evolution watchers, one per captured table.
    schema_watchers: Vec<SchemaWatcher>,
    /// Resolved stop boundary (file, position) for stop.mode=latest/specific.
    stop_boundary: Option<(String, u64)>,
    /// Set when the stream passed the stop boundary; EOF after drain.
    stop_reached: bool,
    /// Timestamp warm-up (milliseconds): discard binlog events older than
    /// this before starting to emit (startup.mode = timestamp).
    skip_until_ts_ms: Option<i64>,
    /// Persistent connection pool shared by snapshot batches and metadata
    /// queries (avoids a fresh pool — and TCP churn — per batch).
    pool: Option<Pool>,
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
            schema_watchers: Vec::new(),
            stop_boundary: None,
            stop_reached: false,
            skip_until_ts_ms: None,
            pool: None,
        }
    }

    /// The reader's cached pool, built on first use.
    fn build_pool(&mut self) -> Pool {
        if self.pool.is_none() {
            self.pool = Some(MySqlCdcSource::build_pool_for(&self.config));
        }
        self.pool.clone().expect("pool just built")
    }

    /// Create and prime one schema watcher per captured table.
    async fn prime_schema_watchers(&mut self) -> anyhow::Result<()> {
        let pool = self.build_pool();
        let mut conn = pool.get_conn().await?;
        let tables = resolve_captured_tables(&mut conn, &self.config).await?;
        for (db, table) in tables {
            let rows: Vec<(String, u32)> = conn
                .exec(
                    "SELECT COLUMN_NAME, ORDINAL_POSITION FROM information_schema.columns \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    (&db, &table),
                )
                .await?;
            let columns: Vec<seatunnel_api::ColumnDef> = rows
                .into_iter()
                .map(|(name, _)| {
                    seatunnel_api::ColumnDef::new(name, seatunnel_api::ColumnType::String)
                })
                .collect();
            let mut watcher =
                SchemaWatcher::new(format!("{}.{}", db, table), &self.config.schema_evolution);
            watcher.prime(columns);
            self.schema_watchers.push(watcher);
        }
        tracing::info!(
            "MySQL CDC: {} schema watcher(s) primed",
            self.schema_watchers.len()
        );
        Ok(())
    }

    /// Emit a pending schema-change event from any table's watcher.
    fn take_schema_change(&mut self) -> Option<PollResult<MySqlCdcOutput>> {
        let event = self
            .schema_watchers
            .iter_mut()
            .find_map(|w| w.take_pending())?;
        Some(PollResult::SchemaChange(Box::new(event)))
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
        if let Some(ts) = state.offset.get("skip_until_ts") {
            // Checkpoint taken during the timestamp warm-up: keep skipping.
            if let Ok(ts) = ts.parse::<i64>() {
                self.skip_until_ts_ms = Some(ts);
            }
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

        // Timestamp warm-up: discard events older than the requested start
        // time; only the offset (and binlog rotation) is tracked.
        if let Some(ts_ms) = self.skip_until_ts_ms {
            if i64::from(event.header().timestamp()) * 1000 < ts_ms {
                tracing::trace!(
                    "MySQL CDC warm-up: skip event type={:?} ts={} pos={}",
                    event.header().event_type(),
                    event.header().timestamp(),
                    event.header().log_pos()
                );
                self.track_rotation(&event);
                return;
            }
            self.skip_until_ts_ms = None;
            tracing::info!(
                "MySQL CDC reader: reached startup.timestamp boundary, streaming live events"
            );
        }

        // Stop boundary (stop.mode = latest | specific | timestamp): events
        // beyond it are not absorbed; the reader EOFs once drained.
        if !self.stop_reached {
            let beyond = match (&self.config.stop_mode, &self.stop_boundary) {
                (MySqlStopMode::Never, _) => false,
                (MySqlStopMode::Latest, _) | (MySqlStopMode::Specific { .. }, _) => {
                    match &self.stop_boundary {
                        Some((file, pos)) => {
                            let cur_file = self.offset.file.as_str();
                            cur_file > file.as_str()
                                || (cur_file == file.as_str()
                                    && (event.header().log_pos() as u64) > *pos)
                        }
                        None => false,
                    }
                }
                (MySqlStopMode::Timestamp { timestamp }, _) => {
                    i64::from(event.header().timestamp()) * 1000 > *timestamp
                }
            };
            if beyond {
                tracing::info!(
                    "MySQL CDC reader: stop boundary reached (mode={:?}) at {}/{}",
                    self.config.stop_mode,
                    self.offset.file,
                    event.header().log_pos()
                );
                self.stop_reached = true;
                return;
            }
        } else {
            return;
        }

        let data = match event.read_data() {
            Ok(Some(d)) => d,
            _ => return,
        };
        match data {
            EventData::TableMapEvent(tme) => {
                self.table_maps.insert(tme.table_id(), tme.into_owned());
            }
            EventData::RowsEvent(rows) => self.absorb_rows_event(rows),
            EventData::QueryEvent(qe) => self.observe_query_event(&qe),
            EventData::RotateEvent(_) => self.track_rotation(&event),
            _ => {}
        }
    }

    /// Update the tracked offset across a binlog rotation so checkpoints
    /// and reconnects stay valid after the server switches files.
    fn track_rotation(&mut self, event: &Event) {
        if let Ok(Some(EventData::RotateEvent(re))) = event.read_data() {
            let name = re.name().to_string();
            if !name.is_empty() {
                self.offset.file = name;
                self.offset.position = re.position();
            }
        }
    }

    /// Fast-path schema evolution: feed captured `ALTER TABLE` statements
    /// from the binlog into the watcher.
    fn observe_query_event(&mut self, qe: &mysql_async::binlog::events::QueryEvent<'_>) {
        if self.schema_watchers.is_empty() {
            return;
        }
        let schema = String::from_utf8_lossy(qe.schema_raw()).to_string();
        let query = qe.query().to_string();
        if !query.to_lowercase().trim_start().starts_with("alter table") {
            return;
        }
        let Some(table) = alter_table_target(&query) else {
            return;
        };
        if !self.config.table_selector.matches(&schema, &table) {
            tracing::debug!("MySQL CDC: ALTER on non-captured table ignored: {}", query);
            return;
        }
        let watcher_id = format!("{}.{}", schema, table);
        if let Some(watcher) = self
            .schema_watchers
            .iter_mut()
            .find(|w| w.table_id == watcher_id)
        {
            watcher.observe_ddl(&query);
        }
    }

    fn absorb_rows_event(&mut self, rows: RowsEventData) {
        let table_id = rows.table_id();
        let num_cols = rows.num_columns() as usize;
        let Some(tme) = self.table_maps.get(&table_id) else {
            return;
        };
        // Only capture events belonging to the selected tables
        // (exact db.table refs, regex patterns, legacy wildcards).
        if !self
            .config
            .table_selector
            .matches(tme.database_name().as_ref(), tme.table_name().as_ref())
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

            // Log official-but-unimplemented options once per reader.

            // Resolve stop.mode: `latest` stops at the position captured
            // right now; `specific` uses the configured offset.
            match self.config.stop_mode.clone() {
                MySqlStopMode::Latest => {
                    self.stop_boundary =
                        Some((self.offset.file.clone(), self.offset.position));
                }
                MySqlStopMode::Specific { file, position } => {
                    if file.is_empty() {
                        anyhow::bail!(
                            "stop.mode=specific requires stop.specific-offset.file"
                        );
                    }
                    self.stop_boundary = Some((file, position));
                }
                _ => {}
            }

            // Schema evolution: subtask 0 owns the binlog stream and the
            // DDL watcher; snapshot-only subtasks finish too quickly to care.
            if self.config.schema_evolution.enabled && self.config.subtask_index == 0 {
                if let Err(e) = self.prime_schema_watchers().await {
                    tracing::warn!("MySQL CDC: schema watcher priming failed: {}", e);
                }
            }

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
                    if file.is_empty() {
                        anyhow::bail!(
                            "startup.mode=specific requires startup.specific.file"
                        );
                    }
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
                MySqlStartupMode::Timestamp { timestamp } => {
                    if timestamp <= 0 {
                        anyhow::bail!(
                            "startup.mode=timestamp requires startup.timestamp (milliseconds since epoch)"
                        );
                    }
                    self.phase = CdcPhase::Incremental;
                    self.splits.clear();
                    self.skip_until_ts_ms = Some(timestamp);
                    // Start from the earliest retained binlog so changes
                    // between `timestamp` and now are captured; older
                    // events are discarded by the warm-up filter. The
                    // baseline GTID set must go too, or the server would
                    // jump straight back to "now".
                    self.offset.gtid_set = None;
                    self.binlog_stream = None;
                    self.stream_broken = false;
                    let pool = self.build_pool();
                    let mut conn = pool.get_conn().await?;
                    // SHOW BINARY LOGS yields (name, size, encrypted); take
                    // the first log's name.
                    let first_log: Option<String> = match conn
                        .query_first::<mysql_async::Row, _>("SHOW BINARY LOGS")
                        .await
                    {
                        Ok(row) => row.and_then(|r| r.get::<Option<String>, _>(0).flatten()),
                        Err(e) => {
                            tracing::warn!("SHOW BINARY LOGS failed: {}", e);
                            None
                        }
                    };
                    match first_log {
                        Some(file) => {
                            self.offset.file = file;
                            self.offset.position = 4;
                        }
                        None => {
                            // No binary logs listed (unusual) — fall back
                            // to the baseline position.
                        }
                    }
                    let stream =
                        Self::open_binlog_stream(conn, &self.config, &self.offset).await?;
                    self.binlog_stream = Some(Box::pin(stream));
                    tracing::info!(
                        "MySQL CDC reader: timestamp startup — skipping binlog events before {}",
                        timestamp
                    );
                    return Ok(());
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
            // Schema-change events take precedence so the sink applies DDL
            // before any row with the new shape is written.
            if let Some(change) = self.take_schema_change() {
                return Ok(change);
            }

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
            let pool = self.build_pool();
            let rows = query_snapshot_batch(&pool, &self.config, &split, last_pk).await?;
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
        if let Some(ts) = self.skip_until_ts_ms {
            offset.insert("skip_until_ts".to_string(), ts.to_string());
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
        // 0. Drained after crossing the stop boundary → bounded capture ends.
        if self.stop_reached {
            tracing::info!("MySQL CDC reader: bounded capture complete (stop.mode={:?})", self.config.stop_mode);
            return Ok(PollResult::EOF);
        }

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

        // 3a. Timestamp warm-up: drain historical events in a tight loop
        // (a per-poll single event would crawl through hundreds of stale
        // entries before reaching the requested start time).
        if self.skip_until_ts_ms.is_some() {
            for _ in 0..TIMESTAMP_WARMUP_DRAIN_BUDGET {
                match self.next_binlog_event_with_timeout(50).await {
                    Ok(Some(())) => {
                        if self.skip_until_ts_ms.is_none() {
                            break;
                        }
                    }
                    Ok(None) => {
                tracing::trace!("MySQL CDC warm-up: idle at tail (no event within budget)");
                break;
            }
            Err(e) => return Err(e),
                }
            }
            if let Some(row) = self.next_buffered_change() {
                return Ok(PollResult::Record(MySqlCdcOutput(row)));
            }
            return Ok(PollResult::Empty);
        }

        // 3b. Read events (bounded block) until at least one row is decoded.
        // Binlog traffic interleaves non-row events (BEGIN/TableMap/XID/GTID)
        // with row events; absorbing a single event per poll made every
        // non-row event surface as `Empty`, so the engine's idle backoff
        // throttled capture to a handful of rows per second. Drain until a
        // row is available, the stop boundary is reached, or the deadline
        // passes.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
        while self.binlog_buffer.is_empty() && !self.stop_reached {
            let remain = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remain.is_zero() {
                break;
            }
            match self
                .next_binlog_event_with_timeout(remain.as_millis() as u64)
                .await?
            {
                Some(()) => {}
                None => break, // timed out or the dump stream ended
            }
        }
        if let Some(row) = self.next_buffered_change() {
            return Ok(PollResult::Record(MySqlCdcOutput(row)));
        }
        Ok(PollResult::Empty)
    }
}

/// Fetch the next keyset-paginated snapshot batch for a split.
/// Returns `(first_pk_of_batch, row)` pairs ordered by the split column.
async fn query_snapshot_batch(
    pool: &Pool,
    config: &MySqlCdcConfig,
    split: &SnapshotSplit,
    last_pk: i64,
) -> anyhow::Result<Vec<(i64, SeatunnelRow)>> {
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
        config.snapshot_fetch_size,
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
        Some(Value::Date(y, m, d, h, min, s, _)) => {
            // DATETIME keeps its time part; pure DATEs render date-only.
            if (h, min, s) == (0, 0, 0) {
                Field::String(format!("{:04}-{:02}-{:02}", y, m, d))
            } else {
                Field::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    y, m, d, h, min, s
                ))
            }
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
        Value::Date(y, m, d, h, min, s, _) => {
            if (*h, *min, *s) == (0, 0, 0) {
                Field::String(format!("{:04}-{:02}-{:02}", y, m, d))
            } else {
                Field::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    y, m, d, h, min, s
                ))
            }
        }
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
            schema_evolution: SchemaEvolutionConfig::default(),
            table_selector: TableSelector::from_legacy("testdb", "test_table"),
            ..Default::default()
        }
    }

    #[test]
    fn test_mysql_startup_mode_parsing() {
        let mk = |pairs: &[(&str, &str)]| {
            let props: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            MySqlCdcConfig::from_config(&ConnectorConfig::new(props)).startup_mode
        };
        assert_eq!(
            mk(&[
                ("startup.mode", "timestamp"),
                ("startup.timestamp", "1667232000000"),
            ]),
            MySqlStartupMode::Timestamp {
                timestamp: 1_667_232_000_000
            }
        );
        assert_eq!(
            mk(&[
                ("startup.mode", "specific"),
                ("startup.specific.file", "binlog.000003"),
                ("startup.specific.pos", "987"),
                ("startup.specific.gtid-set", "aaa:1-5"),
            ]),
            MySqlStartupMode::Specific {
                file: "binlog.000003".to_string(),
                position: 987,
                gtid_set: Some("aaa:1-5".to_string()),
            }
        );
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
    fn multi_table_selection_from_official_options() {
        let props: HashMap<String, String> = [
            ("url", "jdbc:mysql://127.0.0.1:13306/seatunnel"),
            ("username", "root"),
            ("password", "root"),
            ("database-names", "seatunnel"),
            ("table-names", "seatunnel.users_a,seatunnel.users_b"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let cfg = MySqlCdcConfig::from_config(&ConnectorConfig::new(props));
        assert!(cfg.table_selector.matches("seatunnel", "users_a"));
    }

    #[test]
    fn table_pattern_matching() {
        // Legacy single-name selection.
        let cfg = test_config();
        assert!(cfg.table_selector.matches("testdb", "test_table"));
        assert!(!cfg.table_selector.matches("testdb", "other"));

        // Legacy trailing % wildcard.
        let mk = |pairs: &[(&str, &str)]| {
            let props: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            MySqlCdcConfig::from_config(&ConnectorConfig::new(props))
        };
        let cfg = mk(&[("table-name", "events%")]);
        assert!(cfg.table_selector.matches("seatunnel", "events_2026"));
        assert!(!cfg.table_selector.matches("seatunnel", "orders"));

        // Official list selection: table-names entries are db.table pairs.
        let cfg = mk(&[
            ("database-names", "seatunnel,analytics"),
            ("table-names", "seatunnel.users,analytics.orders"),
        ]);
        assert!(cfg.table_selector.matches("seatunnel", "users"));
        assert!(cfg.table_selector.matches("analytics", "orders"));
        assert!(!cfg.table_selector.matches("seatunnel", "orders"));

        // Official regex patterns over db and db.table.
        let cfg = mk(&[
            ("database-pattern", "seatunnel.*"),
            ("table-pattern", "seatunnel.*\\.events_.*"),
        ]);
        assert!(cfg.table_selector.matches("seatunnel", "events_2026"));
        assert!(cfg.table_selector.matches("seatunnel_shard", "events_2026"));
        assert!(!cfg.table_selector.matches("seatunnel", "users"));

        // table-names-config per-table split column override.
        let cfg = mk(&[(
            "table-names-config",
            "[{\"table\": \"seatunnel.users\",\"snapshotSplitColumn\": \"created_at\"}]",
        )]);
        assert_eq!(
            cfg.table_selector.split_column_for("seatunnel", "users"),
            Some("created_at")
        );
    }

    #[test]
    fn url_and_connection_options() {
        let mk = |pairs: &[(&str, &str)]| {
            let props: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            MySqlCdcConfig::from_config(&ConnectorConfig::new(props))
        };
        let cfg = mk(&[("url", "jdbc:mysql://db.host:3307/inventory")]);
        assert_eq!(cfg.hostname, "db.host");
        assert_eq!(cfg.port, 3307);
        assert_eq!(cfg.database_name, "inventory");
        // database-name overrides the URL path when both are set.
        let cfg = mk(&[
            ("url", "jdbc:mysql://db.host:3307/inventory"),
            ("database-name", "other"),
        ]);
        assert_eq!(cfg.database_name, "other");

        // server-id range.
        let cfg = mk(&[("server-id", "5400-5408")]);
        assert_eq!(cfg.server_id, 5400);
        assert_eq!(cfg.server_id_range, 8);

        // snapshot sizing + pool.
        let cfg = mk(&[
            ("snapshot.split.size", "4096"),
            ("snapshot.fetch.size", "256"),
            ("connection.pool.size", "8"),
        ]);
        assert_eq!(cfg.snapshot_split_size, 4096);
        assert_eq!(cfg.snapshot_fetch_size, 256);
        assert_eq!(cfg.connection_pool_size, 8);
    }

    #[test]
    fn official_startup_and_stop_options() {
        let mk = |pairs: &[(&str, &str)]| {
            let props: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            MySqlCdcConfig::from_config(&ConnectorConfig::new(props))
        };
        // Official startup.specific-offset.* keys.
        let cfg = mk(&[
            ("startup.mode", "specific"),
            ("startup.specific-offset.file", "binlog.000004"),
            ("startup.specific-offset.pos", "1234"),
            ("startup.specific-offset.gtid-set", "aaa:1-9"),
        ]);
        assert_eq!(
            cfg.startup_mode,
            MySqlStartupMode::Specific {
                file: "binlog.000004".to_string(),
                position: 1234,
                gtid_set: Some("aaa:1-9".to_string()),
            }
        );
        // Stop modes.
        let cfg = mk(&[("stop.mode", "never")]);
        assert_eq!(cfg.stop_mode, MySqlStopMode::Never);
        let cfg = mk(&[("stop.mode", "latest")]);
        assert_eq!(cfg.stop_mode, MySqlStopMode::Latest);
        let cfg = mk(&[
            ("stop.mode", "specific"),
            ("stop.specific-offset.file", "binlog.000009"),
            ("stop.specific-offset.pos", "99"),
        ]);
        assert_eq!(
            cfg.stop_mode,
            MySqlStopMode::Specific {
                file: "binlog.000009".to_string(),
                position: 99,
            }
        );
        let cfg = mk(&[("stop.mode", "timestamp"), ("stop.timestamp", "1667232000000")]);
        assert_eq!(
            cfg.stop_mode,
            MySqlStopMode::Timestamp {
                timestamp: 1_667_232_000_000
            }
        );
    }
}
