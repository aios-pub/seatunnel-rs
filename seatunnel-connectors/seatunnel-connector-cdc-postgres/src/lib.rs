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

//! PostgreSQL CDC (Change Data Capture) connector.
//!
//! ## Pipeline semantics (mirrors the MySQL connector)
//!
//! ```text
//! open()
//!   ├─ tokio_postgres connect + ping          (fail ⇒ task error)
//!   ├─ ensure publication exists & covers the table
//!   ├─ SELECT pg_current_wal_lsn()            → baseline LSN (metadata)
//!   ├─ cache column names/types               → uniform row layout
//!   ├─ start logical-replication stream       → events buffer while snapshot runs
//!   └─ enumerate snapshot splits              → disjoint id ranges per subtask
//!
//! poll_next()
//!   ├─ SNAPSHOT phase
//!   │    ├─ bounded drain of stream events into the replay buffer
//!   │    └─ keyset-paginated `SELECT … WHERE id ∈ [start,end) AND id > :last`
//!   └─ INCREMENTAL phase
//!        └─ replay buffer first, then follow the live WAL stream forever
//! ```
//!
//! Delivery guarantees: **at-least-once**. With `parallelism > 1`, snapshot
//! ranges are partitioned across subtasks while subtask 0 alone streams the
//! WAL; changes committed during the snapshot window are replayed on top of
//! the snapshot, so nothing is lost (bounded duplicate window). Checkpoint
//! state serializes phase, LSN, split progress and the column order so a
//! restarted task resumes where it stopped.
//!
//! ## Requirements
//! - `wal_level = logical`; user with `REPLICATION` attribute
//! - A publication covering the captured table (auto-created when absent)
//! - Replication slot auto-creation is controlled by `auto-create-slot`
//!   (default **true**, matching Debezium's behavior; disable and provision
//!   out of band in production if slot loss must be a hard failure)

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::Mutex;
use postgres_types::Type;
use rustcdc::core::Operation;
use rustcdc::source::Source as _;
use rustcdc::source::StreamHandle;
use rustcdc::source::postgres::{PostgresConnection, PostgresSourceConfig};
use seatunnel_api::row::{Field, Row, RowKind};
use seatunnel_api::schema::TableSchema;
use seatunnel_api::source::{
    Boundedness, Source,
    source_reader::{PollResult, SourceReader, SourceReaderContext},
    source_split::SourceSplit,
    source_split_enum::SourceSplitEnumeratorContext,
};
use seatunnel_connector_cdc_base::{CdcPhase, IncrementalSplit, SnapshotSplit, Watermark};
use seatunnel_connector_common::ConnectorConfig;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};

/// Maximum change rows buffered while the snapshot phase runs.
const MAX_BUFFERED_STREAM_ROWS: usize = 65_536;

/// Consecutive stream errors tolerated before the task fails.
const STREAM_ERROR_TOLERANCE: u32 = 5;

/// Output row from PostgreSQL CDC.
#[derive(Debug, Clone)]
pub struct PostgresCdcOutput(pub Row);

impl From<PostgresCdcOutput> for Row {
    fn from(val: PostgresCdcOutput) -> Self {
        val.0
    }
}

/// PostgreSQL CDC combined split.
#[derive(Debug, Clone)]
pub enum PostgresCdcSplit {
    Snapshot(SnapshotSplit),
    Incremental(IncrementalSplit),
}

impl SourceSplit for PostgresCdcSplit {
    fn split_id(&self) -> &str {
        match self {
            PostgresCdcSplit::Snapshot(s) => s.split_id(),
            PostgresCdcSplit::Incremental(s) => s.split_id(),
        }
    }
}

/// PostgreSQL Log Sequence Number (LSN).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    pub datum: u64,
}

impl Lsn {
    pub fn new(high: u32, low: u32) -> Self {
        Lsn {
            datum: ((high as u64) << 32) | (low as u64),
        }
    }

    pub fn zero() -> Self {
        Lsn { datum: 0 }
    }

    pub fn from_datum(datum: u64) -> Self {
        Lsn { datum }
    }

    pub fn to_datum(&self) -> u64 {
        self.datum
    }

    pub fn high(&self) -> u32 {
        (self.datum >> 32) as u32
    }

    pub fn low(&self) -> u32 {
        self.datum as u32
    }

    pub fn fmt_hex(&self) -> String {
        format!("{:08X}/{:08X}", self.high(), self.low())
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let high = u32::from_str_radix(parts[0].trim_start_matches('0').max("0"), 16).ok()?;
        let low = u32::from_str_radix(parts[1].trim_start_matches('0').max("0"), 16).ok()?;
        Some(Lsn::new(high, low))
    }
}

impl Default for Lsn {
    fn default() -> Self {
        Lsn::zero()
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fmt_hex())
    }
}

/// PostgreSQL CDC checkpoint state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCdcState {
    pub phase: CdcPhase,
    pub lsn: Lsn,
    pub watermark: Watermark,
    pub offset: HashMap<String, String>,
}

impl Default for PostgresCdcState {
    fn default() -> Self {
        PostgresCdcState {
            phase: CdcPhase::Snapshot,
            lsn: Lsn::zero(),
            watermark: Watermark::Min,
            offset: HashMap::new(),
        }
    }
}

impl PostgresCdcState {
    pub fn new(phase: CdcPhase, lsn: Lsn) -> Self {
        PostgresCdcState {
            phase,
            lsn,
            watermark: Watermark::Min,
            offset: HashMap::new(),
        }
    }

    pub fn with_watermark(mut self, watermark: Watermark) -> Self {
        self.watermark = watermark;
        self
    }
}

/// PostgreSQL CDC startup mode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PostgresStartupMode {
    /// Full snapshot followed by streaming (default).
    #[default]
    Initial,
    /// Snapshot only, stop after the snapshot completes.
    SnapshotOnly,
    /// Stream from the slot's position without a snapshot.
    Earliest,
    /// Stream from now without a snapshot.
    Latest,
    SpecificLsn {
        lsn: Lsn,
    },
    Timestamp {
        timestamp: i64,
    },
}

// ---------------------------------------------------------------------------
// Table selection (official option set)
// ---------------------------------------------------------------------------

/// PostgreSQL table matcher for the official `table-names` /
/// `table-pattern` options. Entries are `db.table` (or `schema.table`,
/// or `db.schema.table`) — the last component is the table, earlier
/// components match either the connected database or the schema.
#[derive(Debug, Clone, Default)]
pub struct PgTableMatcher {
    /// (qualifier, table): qualifier matches schema or database when set.
    exact: Vec<(Option<String>, String)>,
    patterns: Vec<regex::Regex>,
    /// Legacy single-table fallback (schema + table).
    legacy: Option<(String, String)>,
}

impl PgTableMatcher {
    /// Legacy single-table matcher (schema + table).
    pub fn legacy(schema: &str, table: &str) -> Self {
        PgTableMatcher {
            legacy: Some((schema.to_string(), table.to_string())),
            ..Default::default()
        }
    }

    pub fn matches(&self, database: &str, schema: &str, table: &str) -> bool {
        if let Some((legacy_schema, legacy_table)) = &self.legacy {
            if schema == legacy_schema && table == legacy_table {
                return true;
            }
        }
        if self
            .exact
            .iter()
            .any(|(q, t)| t == table && q.as_ref().is_none_or(|q| q == schema || q == database))
        {
            return true;
        }
        let qualified = [
            format!("{}.{}", schema, table),
            format!("{}.{}", database, table),
        ];
        self.patterns
            .iter()
            .any(|re| qualified.iter().any(|q| re.is_match(q)))
    }

    pub fn is_multi(&self) -> bool {
        self.exact.len() + self.patterns.len() > 1
    }
}

/// Parse `jdbc:postgresql://host:port/db` (or `postgres://`) into
/// (host, port, database).
fn parse_pg_jdbc_url(url: &str) -> Option<(String, u16, String)> {
    let rest = url
        .strip_prefix("jdbc:postgresql://")
        .or_else(|| url.strip_prefix("jdbc:postgres://"))
        .or_else(|| url.strip_prefix("postgresql://"))
        .or_else(|| url.strip_prefix("postgres://"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let database = path.split('?').next().unwrap_or("").to_string();
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(5432)),
        None => (authority.to_string(), 5432),
    };
    if host.is_empty() {
        return None;
    }
    Some((host, port, database))
}

/// PostgreSQL CDC configuration.
#[derive(Debug, Clone)]
pub struct PostgresCdcConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database_name: String,
    /// Schema containing the captured table (PostgreSQL "public" by default).
    pub schema_name: String,
    pub table_name: String,
    /// Split key column used for snapshot chunking (defaults to `id`).
    pub split_column: String,
    pub publication_name: String,
    pub slot_name: String,
    /// Create the replication slot automatically when absent.
    pub auto_create_slot: bool,
    pub startup_mode: PostgresStartupMode,
    pub parallelism: usize,
    /// This reader's subtask index / total count — snapshot ranges are
    /// partitioned so each subtask scans a disjoint interval.
    pub subtask_index: usize,
    pub subtask_count: usize,
    /// Schema-evolution settings (PG DDL detected by catalog polling;
    /// pgoutput does not expose parsed DDL through this stack).
    pub schema_evolution: seatunnel_connector_cdc_base::SchemaEvolutionConfig,
    /// Logical decoding plugin (official `decoding.plugin.name`; only
    /// `pgoutput` is supported — others are warned about).
    pub decoding_plugin: String,
    /// Rows per snapshot split (official `snapshot.split.size`, 8096).
    pub snapshot_split_size: i64,
    /// Snapshot page size (official `snapshot.fetch.size`, 1024).
    pub snapshot_fetch_size: i64,
    /// Resolved table selection from `table-names` / `table-pattern`.
    pub table_matcher: PgTableMatcher,
    /// Warnings for official-but-unimplemented options (logged at open).
    pub compat_warnings: Vec<String>,
}

impl Default for PostgresCdcConfig {
    fn default() -> Self {
        PostgresCdcConfig {
            hostname: "localhost".to_string(),
            port: 5432,
            username: "postgres".to_string(),
            password: String::new(),
            database_name: "seatunnel".to_string(),
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            split_column: "id".to_string(),
            publication_name: "seatunnel_pub".to_string(),
            slot_name: "seatunnel_slot".to_string(),
            auto_create_slot: true,
            startup_mode: PostgresStartupMode::Initial,
            parallelism: 4,
            subtask_index: 0,
            subtask_count: 1,
            schema_evolution: seatunnel_connector_cdc_base::SchemaEvolutionConfig::default(),
            decoding_plugin: "pgoutput".to_string(),
            snapshot_split_size: 8096,
            snapshot_fetch_size: 1024,
            table_matcher: PgTableMatcher::legacy("public", "users"),
            compat_warnings: Vec::new(),
        }
    }
}

impl PostgresCdcConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        // `url` (jdbc:postgresql://host:port/db) is the official connection
        // option; hostname/port remain simpler alternatives.
        let (url_host, url_port, url_db) = config
            .get("url")
            .and_then(|u| parse_pg_jdbc_url(u))
            .unwrap_or_default();

        let decoding_plugin = config.get_string(
            "decoding.plugin.name",
            &config.get_string("decoding_plugin_name", "pgoutput"),
        );
        if decoding_plugin != "pgoutput" {
            tracing::warn!(
                "PostgreSQL CDC: decoding.plugin.name='{}' is not supported; \
                 this implementation uses pgoutput exclusively",
                decoding_plugin
            );
        }

        // database-names: the reader connects to a single database; extra
        // entries are validated against it.
        let database_names =
            config.get_string("database-names", &config.get_string("database_names", ""));
        for name in database_names
            .split(',')
            .map(str::trim)
            .filter(|d| !d.is_empty())
        {
            if name != config.get_string("database-name", &url_db) {
                tracing::warn!(
                    "PostgreSQL CDC: database-names entry '{}' ignored — this reader \
                     captures a single database per job ('{}')",
                    name,
                    config.get_string("database-name", &url_db)
                );
            }
        }

        let mut matcher = PgTableMatcher::default();
        // table-names: db.table / schema.table / db.schema.table entries.
        let table_names = config.get_string("table-names", &config.get_string("table_names", ""));
        for qualified in table_names
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            let parts: Vec<&str> = qualified.split('.').collect();
            match parts.as_slice() {
                [table] => matcher.exact.push((None, table.to_string())),
                [qualifier, table] => matcher
                    .exact
                    .push((Some(qualifier.to_string()), table.to_string())),
                [_, schema, table] => matcher
                    .exact
                    .push((Some(schema.to_string()), table.to_string())),
                _ => tracing::warn!(
                    "PostgreSQL CDC: unparseable table-names entry '{}'",
                    qualified
                ),
            }
        }
        // table-pattern: regex over schema.table / database.table.
        let table_pattern =
            config.get_string("table-pattern", &config.get_string("table_pattern", ""));
        if !table_pattern.is_empty() {
            match regex::Regex::new(&table_pattern) {
                Ok(re) => matcher.patterns.push(re),
                Err(e) => tracing::warn!("invalid table-pattern '{}': {}", table_pattern, e),
            }
        }
        if matcher.exact.is_empty() && matcher.patterns.is_empty() {
            matcher.legacy = Some((
                config.get_string("schema-name", "public"),
                config.get_string("table-name", "users"),
            ));
        }

        PostgresCdcConfig {
            hostname: {
                let v = config.get_string("hostname", &url_host);
                if v.is_empty() {
                    "localhost".to_string()
                } else {
                    v
                }
            },
            port: {
                let p = config.get_int("port", -1);
                if p > 0 {
                    p as u16
                } else if url_port > 0 {
                    url_port
                } else {
                    5432
                }
            },
            username: config.get_string("username", "postgres"),
            password: config.get_string("password", ""),
            database_name: {
                let v = config.get_string("database-name", &url_db);
                if v.is_empty() {
                    "seatunnel".to_string()
                } else {
                    v
                }
            },
            schema_name: config.get_string("schema-name", "public"),
            table_name: config.get_string("table-name", "users"),
            split_column: config.get_string("split.column", "id"),
            publication_name: config.get_string("publication-name", "seatunnel_pub"),
            slot_name: config.get_string(
                "slot.name",
                &config.get_string(
                    "slot-name",
                    &config.get_string("slot_name", "seatunnel_slot"),
                ),
            ),
            auto_create_slot: config.get_bool("auto-create-slot", true),
            startup_mode: config
                .get("startup.mode")
                .map(|s| match s.as_str() {
                    "initial" => PostgresStartupMode::Initial,
                    "snapshot" | "snapshot-only" => PostgresStartupMode::SnapshotOnly,
                    "earliest" => PostgresStartupMode::Earliest,
                    "latest" => PostgresStartupMode::Latest,
                    _ => PostgresStartupMode::Initial,
                })
                .unwrap_or(PostgresStartupMode::Initial),
            parallelism: config.get_int("parallelism", 4) as usize,
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            schema_evolution: seatunnel_connector_cdc_base::SchemaEvolutionConfig::from_config(
                config,
            ),
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
            decoding_plugin,
            snapshot_split_size: config
                .get_int(
                    "snapshot.split.size",
                    config.get_int("snapshot_split_size", 8096),
                )
                .max(1),
            snapshot_fetch_size: config
                .get_int(
                    "snapshot.fetch.size",
                    config.get_int("snapshot_fetch_size", 1024),
                )
                .max(1),
            table_matcher: matcher,
            compat_warnings: seatunnel_connector_cdc_base::compatibility_warnings(config),
        }
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.hostname, self.port, self.username, self.password, self.database_name
        )
    }

    /// `"schema"."table"` reference for SQL.
    fn qualified_table(&self) -> String {
        format!("\"{}\".\"{}\"", self.schema_name, self.table_name)
    }

    /// `schema.table` entry for the publication / include list.
    fn qualified_plain(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

/// Extract an i64 split-key value from a row, honoring the column's actual
/// type (int2/int4/int8/serial/numeric-text). Falls back to `default` when
/// the value is NULL or unparseable — never panics.
fn pg_extract_pk(row: &tokio_postgres::Row, idx: usize, ty: &Type, default: i64) -> i64 {
    let parsed: Option<i64> = match *ty {
        Type::INT8 => row.try_get::<_, Option<i64>>(idx).ok().flatten(),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(i64::from),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(i64::from),
        Type::OID => row
            .try_get::<_, Option<u32>>(idx)
            .ok()
            .flatten()
            .map(|v| v as i64),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(|v| v as i64),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(|v| v as i64),
        Type::NUMERIC => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .and_then(|s| s.parse::<i64>().ok()),
        _ => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .and_then(|s| s.trim().parse::<i64>().ok()),
    };
    parsed.unwrap_or(default)
}

/// Convert a PostgreSQL row value to a SeaTunnel `Field` by deserializing directly.
fn postgres_row_value_to_field(
    row: &tokio_postgres::Row,
    col_idx: usize,
    col_type: &Type,
) -> Field {
    match *col_type {
        Type::BOOL => match row.try_get::<_, Option<bool>>(col_idx) {
            Ok(Some(v)) => Field::Bool(v),
            _ => Field::Null,
        },
        Type::INT2 => match row.try_get::<_, Option<i16>>(col_idx) {
            Ok(Some(v)) => Field::Int16(v),
            _ => Field::Null,
        },
        Type::INT4 => match row.try_get::<_, Option<i32>>(col_idx) {
            Ok(Some(v)) => Field::Int32(v),
            _ => Field::Null,
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(col_idx) {
            Ok(Some(v)) => Field::Int64(v),
            _ => Field::Null,
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(col_idx) {
            Ok(Some(v)) => Field::Float32(v),
            _ => Field::Null,
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(col_idx) {
            Ok(Some(v)) => Field::Float64(v),
            _ => Field::Null,
        },
        Type::OID => match row.try_get::<_, Option<u32>>(col_idx) {
            Ok(Some(v)) => Field::UInt32(v),
            _ => Field::Null,
        },
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(col_idx) {
            Ok(Some(v)) => Field::Bytes(v),
            _ => Field::Null,
        },
        // Everything else (TEXT/VARCHAR/NUMERIC/JSON/temporal/network/…) is
        // read through its text representation — lossless enough for CDC
        // transport and avoids panics on exotic types.
        _ => match row.try_get::<_, Option<String>>(col_idx) {
            Ok(Some(v)) => Field::String(v),
            _ => Field::Null,
        },
    }
}

/// PostgreSQL CDC Source.
#[derive(Debug, Clone)]
pub struct PostgresCdcSource {
    config: PostgresCdcConfig,
    schema: Option<TableSchema>,
}

impl PostgresCdcSource {
    pub fn new(config: PostgresCdcConfig, schema: Option<TableSchema>) -> Self {
        PostgresCdcSource { config, schema }
    }

    pub fn from_config(config: &ConnectorConfig, schema: Option<TableSchema>) -> Self {
        PostgresCdcSource::new(PostgresCdcConfig::from_config(config), schema)
    }

    pub fn config(&self) -> &PostgresCdcConfig {
        &self.config
    }
}

impl Source for PostgresCdcSource {
    type Output = PostgresCdcOutput;
    type Split = PostgresCdcSplit;
    type State = PostgresCdcState;

    fn enumerate_splits(
        &self,
        _context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        // Synchronous trait surface: run the async enumeration on the current
        // runtime (or a temporary one outside tokio).
        let ranges = {
            let config = self.config.clone();
            let schema = self.config.schema_name.clone();
            let table = self.config.table_name.clone();
            tokio_block_on(
                async move { enumerate_snapshot_ranges(&config, &schema, &table).await },
            )?
        };
        Ok(ranges
            .into_iter()
            .map(|(start, end)| {
                PostgresCdcSplit::Snapshot(SnapshotSplit::new(
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
        Ok(Box::new(PostgresCdcReader::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_reader(
        &self,
        _context: SourceReaderContext,
        state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        let mut reader = PostgresCdcReader::new(self.config.clone(), self.schema.clone());
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

/// Compute this subtask's disjoint snapshot ranges from the live table:
/// `[MIN(split_col), MAX(split_col)]` sliced into `subtask_count` intervals,
/// of which only `subtask_index`'s slice is returned (as [start,end) pairs
/// further chunked into ≤ snapshot.fetch.size spans).
async fn enumerate_snapshot_ranges(
    config: &PostgresCdcConfig,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<(i64, i64)>> {
    let (client, connection) = tokio_postgres::connect(&config.connection_string(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!("postgres enumeration connection closed: {}", e);
        }
    });
    let table_ref = format!("\"{}\".\"{}\"", schema, table);

    let sql = format!(
        // Cast to bigint: MIN/MAX over an int4 column would otherwise return
        // int4 and tokio_postgres refuses cross-type reads.
        "SELECT MIN(\"{}\")::bigint, MAX(\"{}\")::bigint FROM {}",
        config.split_column, config.split_column, table_ref
    );
    let row = client
        .query_one(&sql, &[])
        .await
        .map_err(|e| anyhow::anyhow!("snapshot range query failed: {}", e))?;
    let min_id: Option<i64> = row.get(0);
    let max_id: Option<i64> = row.get(1);

    let (Some(min_id), Some(max_id)) = (min_id, max_id) else {
        tracing::info!(
            "PostgreSQL CDC: table {} is empty; a single empty snapshot split is created",
            table_ref
        );
        return Ok(vec![(0, 1)]);
    };

    let count = config.subtask_count.max(1);
    let span = (max_id - min_id + 1).max(1);
    let by_parallelism = (span + count as i64 - 1) / count as i64;
    // snapshot.split.size caps the per-subtask span, mirroring MySQL.
    let chunk = by_parallelism.min(config.snapshot_split_size.max(1)).max(1);
    let idx = config.subtask_index.min(count - 1) as i64;
    let range_start = min_id + idx * chunk;
    let range_end = (range_start + chunk).min(max_id + 1);
    if range_start > max_id {
        tracing::info!(
            "PostgreSQL CDC: subtask {}/{} has an empty snapshot range",
            config.subtask_index,
            count
        );
        return Ok(vec![(0, 1)]);
    }

    let mut splits = Vec::new();
    let mut cursor = range_start;
    while cursor < range_end {
        let end = (cursor + config.snapshot_fetch_size).min(range_end);
        splits.push((cursor, end));
        cursor = end;
    }
    tracing::info!(
        "PostgreSQL CDC: subtask {}/{} enumerated {} split(s) for {} covering ids [{}, {}]",
        config.subtask_index,
        count,
        splits.len(),
        table_ref,
        range_start,
        range_end
    );
    Ok(splits)
}

/// Resolve concrete (schema, table) pairs matching the configured
/// selection via the catalog.
async fn resolve_pg_tables(config: &PostgresCdcConfig) -> anyhow::Result<Vec<(String, String)>> {
    let (client, connection) = tokio_postgres::connect(&config.connection_string(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!("postgres table-resolution connection closed: {}", e);
        }
    });
    let rows = client
        .query(
            "SELECT table_schema, table_name FROM information_schema.tables \\
             WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')",
            &[],
        )
        .await?;
    let mut matched = Vec::new();
    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        if config
            .table_matcher
            .matches(&config.database_name, &schema, &table)
        {
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

/// A decoded change row waiting in the replay buffer.
#[derive(Debug, Clone)]
struct BufferedChange {
    row: Row,
}

/// PostgreSQL CDC Source reader.
pub struct PostgresCdcReader {
    config: PostgresCdcConfig,
    #[allow(dead_code)] // retained for future schema-aware serialization
    schema: Option<TableSchema>,
    phase: CdcPhase,
    /// Remaining snapshot ranges `[start, end)` with keyset cursors.
    pending_ranges: VecDeque<(i64, i64)>,
    /// Multi-table capture: remaining tables to snapshot after the current
    /// one (qualified `schema.table`), consumed sequentially.
    pending_tables: VecDeque<String>,
    /// (schema, table) currently being snapshotted.
    current_table: (String, String),
    /// Per-table cached layouts for streaming decode (schema.table →
    /// (column names, types)).
    layouts: HashMap<String, (Vec<String>, Vec<Type>)>,
    last_pk: i64,
    current_idx: Cell<usize>,
    lsn: Lsn,
    watermark: Watermark,
    /// Admin/snapshot connection (also used for publication provisioning).
    admin_client: Arc<Mutex<Option<Arc<Client>>>>,
    /// Schema-evolution watcher (catalog polling).
    schema_watcher: Option<seatunnel_connector_cdc_base::SchemaWatcher>,
    /// Cached column order so snapshot rows and streamed events align.
    columns: Vec<String>,
    column_types: Vec<Type>,
    /// Rustcdc connection for logical replication streaming.
    rustcdc_conn: Option<PostgresConnection>,
    /// Rustcdc stream handle for consuming CDC events.
    stream_handle: Option<Box<dyn StreamHandle>>,
    /// Changes decoded from stream events awaiting emission (ordered).
    stream_buffer: VecDeque<BufferedChange>,
    stream_errors: u32,
    /// True once the reader has finished its snapshot and should stop.
    snapshot_only_done: bool,
}

impl PostgresCdcReader {
    pub fn new(config: PostgresCdcConfig, schema: Option<TableSchema>) -> Self {
        PostgresCdcReader {
            config,
            schema,
            phase: CdcPhase::Snapshot,
            pending_ranges: VecDeque::new(),
            pending_tables: VecDeque::new(),
            current_table: (String::new(), String::new()),
            layouts: HashMap::new(),
            last_pk: 0,
            current_idx: Cell::new(0),
            lsn: Lsn::zero(),
            watermark: Watermark::Min,
            admin_client: Arc::new(Mutex::new(None)),
            schema_watcher: None,
            columns: Vec::new(),
            column_types: Vec::new(),
            rustcdc_conn: None,
            stream_handle: None,
            stream_buffer: VecDeque::new(),
            stream_errors: 0,
            snapshot_only_done: false,
        }
    }

    /// Apply previously snapshotted state (checkpoint restore path).
    pub fn apply_state(&mut self, state: PostgresCdcState) {
        self.phase = state.phase;
        self.lsn = state.lsn;
        self.watermark = state.watermark;
        if let Some(idx) = state.offset.get("current_idx") {
            self.current_idx.set(idx.parse().unwrap_or(0));
        }
        if let Some(Ok(cols)) = state
            .offset
            .get("columns")
            .map(|s| serde_json::from_str::<Vec<String>>(s))
        {
            self.columns = cols;
        }
        if let Some(Ok(pk)) = state.offset.get("last_pk").map(|s| s.parse::<i64>()) {
            self.last_pk = pk;
        }
        if let Some(Ok(ranges)) = state
            .offset
            .get("ranges")
            .map(|s| serde_json::from_str::<Vec<(String, String)>>(s))
        {
            self.pending_ranges = ranges
                .into_iter()
                .map(|(a, b)| (a.parse::<i64>().unwrap_or(0), b.parse::<i64>().unwrap_or(0)))
                .collect();
        }
        if let Some(Ok(tables)) = state
            .offset
            .get("pending_tables")
            .map(|s| serde_json::from_str::<Vec<String>>(s))
        {
            self.pending_tables = tables.into_iter().collect();
        }
        if let Some(current) = state.offset.get("current_table") {
            if let Some((schema, table)) = current.rsplit_once('.') {
                self.current_table = (schema.to_string(), table.to_string());
            }
        }
        if self.phase == CdcPhase::Incremental {
            self.pending_ranges.clear();
        }
        tracing::info!(
            "PostgreSQL CDC reader: restored state phase={} lsn={} ranges={}",
            self.phase,
            self.lsn,
            self.pending_ranges.len()
        );
    }

    /// Deserialize + apply a serialized [`PostgresCdcState`].
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: PostgresCdcState = serde_json::from_slice(bytes)
            .map_err(|e| anyhow::anyhow!("bad PG CDC state: {}", e))?;
        self.apply_state(state);
        Ok(())
    }

    async fn connect_admin(&mut self) -> anyhow::Result<Arc<Client>> {
        if let Some(client) = self.admin_client.lock().clone() {
            return Ok(client);
        }
        let (client, connection) =
            tokio_postgres::connect(&self.config.connection_string(), NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!("postgres admin connection closed: {}", e);
            }
        });
        let client = Arc::new(client);
        *self.admin_client.lock() = Some(Arc::clone(&client));
        Ok(client)
    }

    /// Ensure the publication exists and covers the captured table.
    ///
    /// Parallel subtasks may race here, so check/create runs under a
    /// session-level advisory lock keyed on the publication name (auto
    /// released if the session dies).
    async fn ensure_publication(client: &Client, config: &PostgresCdcConfig) -> anyhow::Result<()> {
        let lock_key = format!("seatunnel-pub-{}", config.publication_name);
        client
            .query_one("SELECT pg_advisory_lock(hashtext($1), 0)", &[&lock_key])
            .await?;

        let outcome = Self::ensure_publication_locked(client, config).await;

        // Always release, even when the body failed.
        if let Err(e) = client
            .query_one("SELECT pg_advisory_unlock(hashtext($1), 0)", &[&lock_key])
            .await
        {
            tracing::warn!("PostgreSQL CDC: failed to release publication lock: {}", e);
        }
        outcome
    }

    async fn ensure_publication_locked(
        client: &Client,
        config: &PostgresCdcConfig,
    ) -> anyhow::Result<()> {
        let exists = client
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1",
                &[&config.publication_name],
            )
            .await?
            .is_some();

        if !exists {
            tracing::info!(
                "PostgreSQL CDC: creating publication '{}' for table {}",
                config.publication_name,
                config.qualified_table()
            );
            client
                .execute(
                    &format!(
                        "CREATE PUBLICATION \"{}\" FOR TABLE {}",
                        config.publication_name,
                        config.qualified_table()
                    ),
                    &[],
                )
                .await?;
            return Ok(());
        }

        // Publication exists — make sure our table is part of it.
        let member = client
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_publication_tables \
                 WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
                &[
                    &config.publication_name,
                    &config.schema_name,
                    &config.table_name,
                ],
            )
            .await?
            .is_some();
        if !member {
            tracing::info!(
                "PostgreSQL CDC: adding {} to publication '{}'",
                config.qualified_table(),
                config.publication_name
            );
            client
                .execute(
                    &format!(
                        "ALTER PUBLICATION \"{}\" ADD TABLE {}",
                        config.publication_name,
                        config.qualified_table()
                    ),
                    &[],
                )
                .await?;
        }
        Ok(())
    }

    /// Switch the snapshot target: refresh the cached layout and load this
    /// subtask's ranges for the new table.
    async fn switch_snapshot_table(
        &mut self,
        client: &Arc<Client>,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("PostgreSQL CDC: snapshot advancing to {}.{}", schema, table);
        self.current_table = (schema.to_string(), table.to_string());
        self.cache_columns_for(client, schema, table).await?;
        let ranges = enumerate_snapshot_ranges(&self.config, schema, table).await?;
        self.pending_ranges = ranges.into_iter().collect();
        Ok(())
    }

    /// Prime the schema watcher baseline (subtask 0 only).
    async fn prime_schema_watcher(&mut self) {
        if !self.config.schema_evolution.enabled || self.config.subtask_index != 0 {
            return;
        }
        let mut watcher = seatunnel_connector_cdc_base::SchemaWatcher::new(
            self.config.qualified_table(),
            &self.config.schema_evolution,
        );
        // Bind the guard clone first: argument temporaries would otherwise
        // live across the await and make the future non-Send.
        let admin = self.admin_client.lock().clone();
        match fetch_pg_column_defs(admin, &self.config.schema_name, &self.config.table_name).await {
            Ok(defs) if !defs.is_empty() => watcher.prime(defs),
            Ok(_) => tracing::warn!("PostgreSQL CDC: schema watcher found no columns"),
            Err(e) => tracing::warn!("PostgreSQL CDC: schema watcher priming failed: {}", e),
        }
        self.schema_watcher = Some(watcher);
    }

    /// Poll the schema watcher; on change refreshes the cached column
    /// layout (used to decode streamed rows) and returns the event.
    async fn poll_schema_watcher(&mut self) -> Option<seatunnel_api::SchemaChangeEvent> {
        // Split the borrows: watcher vs admin client vs cached layout.
        let Self {
            schema_watcher,
            admin_client,
            config,
            columns,
            column_types,
            ..
        } = self;
        let watcher = schema_watcher.as_mut()?;
        let admin = admin_client.lock().clone();
        let schema_name = config.schema_name.clone();
        let table_name = config.table_name.clone();
        let admin_fetch = admin.clone();
        let result =
            watcher
                .poll(|| async move {
                    fetch_pg_column_defs(admin_fetch, &schema_name, &table_name).await
                })
                .await;
        if let Err(e) = result {
            tracing::debug!("PostgreSQL CDC: schema poll failed: {}", e);
            return None;
        }
        let event = watcher.take_pending()?;
        if let Some(client) = admin {
            let sql = format!("SELECT * FROM {} LIMIT 0", config.qualified_table());
            match client.prepare(&sql).await {
                Ok(stmt) => {
                    *columns = stmt
                        .columns()
                        .iter()
                        .map(|c| c.name().to_string())
                        .collect();
                    *column_types = stmt.columns().iter().map(|c| c.type_().clone()).collect();
                }
                Err(e) => tracing::warn!("PostgreSQL CDC: column cache refresh failed: {}", e),
            }
        }
        Some(event)
    }

    /// Cache the layout of an arbitrary table (multi-table capture).
    async fn cache_columns_for(
        &mut self,
        client: &Arc<Client>,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<()> {
        let sql = format!("SELECT * FROM \"{}\".\"{}\" LIMIT 0", schema, table);
        let stmt = client.prepare(&sql).await?;
        let names: Vec<String> = stmt
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        let types: Vec<Type> = stmt.columns().iter().map(|c| c.type_().clone()).collect();
        if names.is_empty() {
            anyhow::bail!("table {}.{} has no columns", schema, table);
        }
        if self.current_table == (schema.to_string(), table.to_string()) {
            self.columns = names.clone();
            self.column_types = types.clone();
        }
        self.layouts
            .insert(format!("{}.{}", schema, table), (names, types));
        Ok(())
    }

    /// Cache column names/types so snapshot and streamed rows share layout.
    async fn cache_columns(&mut self, client: &Client) -> anyhow::Result<()> {
        let sql = format!("SELECT * FROM {} LIMIT 0", self.config.qualified_table());
        let stmt = client.prepare(&sql).await?;
        self.columns = stmt
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        self.column_types = stmt.columns().iter().map(|c| c.type_().clone()).collect();
        if self.columns.is_empty() {
            anyhow::bail!("table {} has no columns", self.config.qualified_table());
        }
        Ok(())
    }

    async fn baseline_lsn(&mut self, client: &Client) -> anyhow::Result<()> {
        let row = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?;
        let txt: String = row.get(0);
        if let Some(lsn) = Lsn::from_hex(&txt) {
            self.lsn = lsn;
            tracing::info!("PostgreSQL CDC reader: baseline LSN {}", txt);
        }
        Ok(())
    }

    fn build_rustcdc_config(&self) -> PostgresSourceConfig {
        use rustcdc::TransportConfig;
        use rustcdc::core::SecretString;
        PostgresSourceConfig {
            host: self.config.hostname.clone(),
            port: self.config.port,
            user: self.config.username.clone(),
            password: SecretString::from(self.config.password.clone()),
            database: self.config.database_name.clone(),
            replication_slot_name: self.config.slot_name.clone(),
            publication_name: self.config.publication_name.clone(),
            // Docker/local PostgreSQL has no TLS; production deployments
            // should prefer the TLS path (expose via config when needed).
            transport: TransportConfig::Plaintext,
            create_replication_slot_if_missing: self.config.auto_create_slot,
            table_include_list: vec![self.config.qualified_plain()],
            ..Default::default()
        }
    }

    async fn start_stream(&mut self) -> anyhow::Result<()> {
        let pg_config = self.build_rustcdc_config();
        let mut conn = PostgresConnection::new(pg_config);
        conn.connect()
            .await
            .map_err(|e| anyhow::anyhow!("rustcdc connect failed: {}", e))?;
        let handle = conn
            .start_stream(None)
            .await
            .map_err(|e| anyhow::anyhow!("replication stream failed: {}", e))?;
        self.rustcdc_conn = Some(conn);
        self.stream_handle = Some(handle);
        self.stream_errors = 0;
        tracing::info!(
            "PostgreSQL CDC reader: stream started (slot={}, publication={})",
            self.config.slot_name,
            self.config.publication_name
        );
        Ok(())
    }

    /// Decode one stream event into the replay buffer using the cached
    /// column order, tracking the LSN for checkpoints.
    fn absorb_event(&mut self, event: rustcdc::Event) {
        if !event.source.offset.is_empty() {
            if let Some(new_lsn) = Lsn::from_hex(&event.source.offset) {
                self.lsn = new_lsn;
            }
        }
        let image = match event.op {
            Operation::Insert | Operation::Read => event.after.map(|d| (RowKind::Insert, d)),
            Operation::Update => event.after.map(|d| (RowKind::UpdateAfter, d)),
            Operation::Delete => event.before.map(|d| (RowKind::Delete, d)),
            _ => None,
        };
        let Some((kind, data)) = image else {
            return;
        };
        // Multi-table capture: filter by the configured selection.
        let schema = event
            .schema
            .clone()
            .unwrap_or_else(|| self.config.schema_name.clone());
        let table = event.table.clone();
        if !self
            .config
            .table_matcher
            .matches(&self.config.database_name, &schema, &table)
        {
            return;
        }
        // Layout per table (falls back to the primary table's cache).
        let layout_key = format!("{}.{}", schema, table);
        let (columns, _types) = self
            .layouts
            .get(&layout_key)
            .cloned()
            .unwrap_or_else(|| (self.columns.clone(), self.column_types.clone()));
        let Some(obj) = data.as_object() else {
            return;
        };
        // Layout identical to snapshot rows: cached column order.
        let width = columns.len().max(obj.len());
        let mut row = Row::new(kind, width);
        for (i, col) in columns.iter().enumerate() {
            row.set(
                i,
                obj.get(col).map(json_val_to_field).unwrap_or(Field::Null),
            );
        }
        // Columns unknown at cache time (schema drift) are appended.
        for (i, (key, val)) in obj.iter().enumerate() {
            if !columns.iter().any(|c| c == key) {
                let idx = columns.len() + i;
                if idx < width {
                    row.set(idx, json_val_to_field(val));
                }
            }
        }
        self.stream_buffer.push_back(BufferedChange { row });
    }

    /// Pull already-available stream events into the replay buffer with a
    /// small wall-clock budget so snapshot reads are not starved.
    async fn drain_stream_nonblocking(&mut self) {
        if self.stream_handle.is_none() || self.stream_buffer.len() >= MAX_BUFFERED_STREAM_ROWS {
            return;
        }
        match self.stream_handle.as_mut().unwrap().next_events(10).await {
            Ok(events) => {
                self.stream_errors = 0;
                for event in events {
                    self.absorb_event(event);
                }
            }
            Err(e) => {
                self.stream_errors += 1;
                tracing::warn!(
                    "PostgreSQL CDC stream error ({}/{}): {}",
                    self.stream_errors,
                    STREAM_ERROR_TOLERANCE,
                    e
                );
            }
        }
    }

    /// Blocking-with-budget read used in the incremental phase.
    async fn pump_stream(&mut self, timeout_ms: u64) -> anyhow::Result<()> {
        let Some(handle) = self.stream_handle.as_mut() else {
            return Ok(());
        };
        match handle.next_events(timeout_ms).await {
            Ok(events) => {
                self.stream_errors = 0;
                for event in events {
                    self.absorb_event(event);
                }
            }
            Err(e) => {
                self.stream_errors += 1;
                if self.stream_errors >= STREAM_ERROR_TOLERANCE {
                    anyhow::bail!(
                        "logical replication stream failed {} times consecutively: {}",
                        self.stream_errors,
                        e
                    );
                }
                tracing::warn!("PostgreSQL CDC stream error: {}", e);
            }
        }
        Ok(())
    }
}

impl SourceReader for PostgresCdcReader {
    type Output = PostgresCdcOutput;
    type Split = PostgresCdcSplit;

    fn open(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!(
                "PostgreSQL CDC reader opening: {}.{} slot={} mode={:?}",
                self.config.database_name,
                self.config.table_name,
                self.config.slot_name,
                self.config.startup_mode
            );

            let resuming_incremental =
                self.phase == CdcPhase::Incremental && self.lsn != Lsn::zero();

            for warning in &self.config.compat_warnings {
                tracing::warn!("PostgreSQL CDC: {}", warning);
            }

            // Admin/snapshot connection — fail loudly when unreachable.
            let client = self.connect_admin().await?;

            // Provision replication prerequisites (idempotent).
            Self::ensure_publication(&client, &self.config).await?;

            // Uniform row layout for both phases.
            self.cache_columns(&client).await?;

            // Schema evolution: baseline the watcher after the layout cache.
            self.prime_schema_watcher().await;

            // Baseline WAL position (metadata for observability/state).
            if !resuming_incremental {
                self.baseline_lsn(&client).await?;
            } else {
                tracing::info!(
                    "PostgreSQL CDC reader: resuming from checkpoint LSN {}",
                    self.lsn
                );
            }

            // Startup-mode shortcuts: streaming-only readers skip snapshot.
            if !resuming_incremental {
                match self.config.startup_mode {
                    PostgresStartupMode::Latest
                    | PostgresStartupMode::Earliest
                    | PostgresStartupMode::SpecificLsn { .. } => {
                        if let PostgresStartupMode::SpecificLsn { .. } = self.config.startup_mode {
                            tracing::warn!(
                                "PostgreSQL CDC: startup.mode=specific-lsn delegates resume positioning to the slot"
                            );
                        }
                        self.phase = CdcPhase::Incremental;
                        self.pending_ranges.clear();
                    }
                    PostgresStartupMode::Timestamp { .. } => {
                        tracing::warn!(
                            "PostgreSQL CDC: startup.mode=timestamp not supported yet, using initial"
                        );
                    }
                    PostgresStartupMode::Initial | PostgresStartupMode::SnapshotOnly => {}
                }
            } else {
                self.phase = CdcPhase::Incremental;
                self.pending_ranges.clear();
            }

            let streams_wal = self.config.subtask_index == 0;

            // Start the WAL stream before the snapshot so changes committed
            // during the scan are buffered and replayed afterwards.
            if streams_wal && !matches!(self.config.startup_mode, PostgresStartupMode::SnapshotOnly)
            {
                self.start_stream().await?;
            }

            // Seed snapshot splits unless a checkpoint supplied them or we
            // are streaming-only. Multi-table selections are snapshotted
            // table by table.
            if self.phase == CdcPhase::Snapshot && self.pending_ranges.is_empty() {
                let matched = resolve_pg_tables(&self.config).await?;
                let (first, rest): (Vec<_>, Vec<_>) = if matched.is_empty() {
                    (
                        vec![(
                            self.config.schema_name.clone(),
                            self.config.table_name.clone(),
                        )],
                        vec![],
                    )
                } else {
                    (vec![matched[0].clone()], matched[1..].to_vec())
                };
                if !rest.is_empty() {
                    tracing::info!(
                        "PostgreSQL CDC: {} table(s) selected for snapshot",
                        matched.len()
                    );
                }
                for (schema, table) in &rest {
                    self.pending_tables
                        .push_back(format!("{}.{}", schema, table));
                }
                if let Some((schema, table)) = first.first() {
                    self.current_table = (schema.clone(), table.clone());
                    if !self.layouts.contains_key(&format!("{}.{}", schema, table)) {
                        self.cache_columns_for(&client, schema, table).await?;
                    }
                    let ranges = enumerate_snapshot_ranges(&self.config, schema, table).await?;
                    self.pending_ranges.extend(ranges);
                }
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
            // Keep the WAL socket drained while chunk-scanning the table.
            self.drain_stream_nonblocking().await;

            // Emit buffered snapshot rows first.
            if let Some(change) = self.stream_buffer.pop_front() {
                return Ok(PollResult::Record(PostgresCdcOutput(change.row)));
            }

            if let Some((start, end)) = self.pending_ranges.front().copied() {
                let client = self.connect_admin().await?;
                let table_ref =
                    format!("\"{}\".\"{}\"", self.current_table.0, self.current_table.1);
                let sql = format!(
                    "SELECT * FROM {} WHERE \"{}\" >= {} AND \"{}\" < {} AND \"{}\" > {} \
                     ORDER BY \"{}\" ASC LIMIT {}",
                    table_ref,
                    self.config.split_column,
                    start,
                    self.config.split_column,
                    end,
                    self.config.split_column,
                    self.last_pk,
                    self.config.split_column,
                    self.config.snapshot_fetch_size,
                );
                let rows = client.query(&sql, &[]).await?;
                if rows.is_empty() {
                    self.pending_ranges.pop_front();
                    self.last_pk = 0;
                    self.current_idx.set(self.current_idx.get() + 1);
                    if self.pending_ranges.is_empty() {
                        // Multi-table capture: advance to the next table.
                        if let Some(next) = self.pending_tables.pop_front() {
                            if let Some((schema, table)) = next.rsplit_once('.') {
                                self.switch_snapshot_table(&client, schema, table).await?;
                            }
                        }
                    }
                    return Ok(PollResult::Empty);
                }
                let types: Vec<Type> = rows[0]
                    .columns()
                    .iter()
                    .map(|c| c.type_().clone())
                    .collect();
                // Split-key cursor: read via the column's real type — int4
                // ids are common and i64 reads would fail (or panic).
                let pk_idx = 0;
                let new_last_pk =
                    pg_extract_pk(&rows[rows.len() - 1], pk_idx, &types[pk_idx], self.last_pk);
                for r in rows.iter() {
                    let field_count = r.columns().len();
                    let mut row = Row::new(RowKind::Insert, field_count);
                    for (j, ty) in types.iter().enumerate().take(field_count) {
                        row.set(j, postgres_row_value_to_field(r, j, ty));
                    }
                    self.stream_buffer.push_back(BufferedChange { row });
                }
                self.last_pk = new_last_pk;
                if let Some(change) = self.stream_buffer.pop_front() {
                    return Ok(PollResult::Record(PostgresCdcOutput(change.row)));
                }
                return Ok(PollResult::Empty);
            }

            // All snapshot splits consumed.
            if self.config.subtask_index != 0
                || matches!(self.config.startup_mode, PostgresStartupMode::SnapshotOnly)
            {
                tracing::info!(
                    "PostgreSQL CDC reader: subtask {}/{} snapshot complete, EOF (streaming handled by subtask 0)",
                    self.config.subtask_index,
                    self.config.subtask_count
                );
                self.snapshot_only_done = true;
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
        offset.insert("lsn".to_string(), self.lsn.fmt_hex());
        offset.insert("slot_name".to_string(), self.config.slot_name.clone());
        offset.insert(
            "current_idx".to_string(),
            self.current_idx.get().to_string(),
        );
        offset.insert("last_pk".to_string(), self.last_pk.to_string());
        if !self.columns.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.columns) {
                offset.insert("columns".to_string(), json);
            }
        }
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
        if !self.pending_tables.is_empty() {
            let tables: Vec<String> = self.pending_tables.iter().cloned().collect();
            if let Ok(json) = serde_json::to_string(&tables) {
                offset.insert("pending_tables".to_string(), json);
            }
        }
        if !self.current_table.0.is_empty() {
            offset.insert(
                "current_table".to_string(),
                format!("{}.{}", self.current_table.0, self.current_table.1),
            );
        }
        let state = PostgresCdcState {
            phase: self.phase,
            lsn: self.lsn,
            watermark: self.watermark,
            offset,
        };
        Box::pin(async move { serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e)) })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!("PostgreSQL CDC reader: adding {} splits", splits.len());
        for split in splits {
            if let PostgresCdcSplit::Snapshot(s) = split {
                if let (Ok(a), Ok(b)) = (s.start_key.parse::<i64>(), s.end_key.parse::<i64>()) {
                    self.pending_ranges.push_back((a, b));
                }
            }
        }
    }

    fn handle_no_more_splits(&mut self) {
        self.phase = CdcPhase::Incremental;
        self.watermark = match self.watermark {
            Watermark::Min => Watermark::Value(self.last_pk.max(0)),
            w => w,
        };
        tracing::info!(
            "PostgreSQL CDC reader: transitioning to incremental phase (replay buffer={})",
            self.stream_buffer.len()
        );
    }

    fn close(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.stream_handle = None;
            self.rustcdc_conn = None;
            tracing::info!("PostgreSQL CDC reader closed");
            Ok(())
        })
    }
}

impl PostgresCdcReader {
    /// Incremental phase pump: replay buffer → live WAL stream → Empty.
    async fn poll_incremental(&mut self) -> anyhow::Result<PollResult<PostgresCdcOutput>> {
        if let Some(change) = self.stream_buffer.pop_front() {
            return Ok(PollResult::Record(PostgresCdcOutput(change.row)));
        }
        if self.snapshot_only_done {
            return Ok(PollResult::EOF);
        }
        // Bounded block keeps latency low without busy-spinning the loop.
        self.pump_stream(250).await?;
        if let Some(change) = self.stream_buffer.pop_front() {
            return Ok(PollResult::Record(PostgresCdcOutput(change.row)));
        }
        // Schema evolution: catalog poll (interval-bounded), emitted before
        // any row with the new shape.
        if let Some(event) = self.poll_schema_watcher().await {
            return Ok(PollResult::SchemaChange(Box::new(event)));
        }
        Ok(PollResult::Empty)
    }
}

/// Convert a serde_json::Value to a Seatunnel Field.
fn json_val_to_field(val: &serde_json::Value) -> Field {
    match val {
        serde_json::Value::Null => Field::Null,
        serde_json::Value::Bool(b) => Field::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Field::Int64(i)
            } else if let Some(u) = n.as_u64() {
                Field::UInt64(u)
            } else if let Some(f) = n.as_f64() {
                Field::Float64(f)
            } else {
                Field::Null
            }
        }
        serde_json::Value::String(s) => Field::String(s.clone()),
        serde_json::Value::Array(arr) => {
            let fields: Vec<Field> = arr.iter().map(json_val_to_field).collect();
            Field::Row(fields)
        }
        serde_json::Value::Object(_) => Field::String(val.to_string()),
    }
}

/// Fetch table columns from the pg catalog (schema-evolution watcher input).
async fn fetch_pg_column_defs(
    client: Option<Arc<Client>>,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<Vec<seatunnel_api::ColumnDef>> {
    let client = client.ok_or_else(|| anyhow::anyhow!("admin client not connected"))?;
    let sql = "SELECT a.attname AS name, t.typname AS data_type, \
               NOT a.attnotnull AS nullable, \
               COALESCE((SELECT ix.indisprimary FROM pg_index ix \
                         WHERE ix.indrelid = a.attrelid \
                           AND a.attnum = ANY(ix.indkey) LIMIT 1), false) AS is_primary \
        FROM pg_attribute a \
        JOIN pg_class c ON a.attrelid = c.oid \
        JOIN pg_namespace n ON c.relnamespace = n.oid \
        JOIN pg_type t ON a.atttypid = t.oid \
        WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped \
        ORDER BY a.attnum";
    let rows = client.query(sql, &[&schema_name, &table_name]).await?;
    use seatunnel_api::schema::DatabaseDialect;
    let dialect = seatunnel_api::schema::PostgresDialect;
    let mut defs = Vec::with_capacity(rows.len());
    for row in &rows {
        let name: String = row.get(0);
        let data_type: String = row.get(1);
        let nullable: bool = row.get(2);
        let is_primary: bool = row.get(3);
        defs.push(
            seatunnel_api::ColumnDef::new(name, dialect.map_type(&data_type, None, None))
                .nullable(nullable)
                .with_primary_key(is_primary)
                .source_type(data_type),
        );
    }
    Ok(defs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PostgresCdcConfig {
        PostgresCdcConfig {
            hostname: "127.0.0.1".into(),
            port: 1, // loopback port 1 is never our database
            username: "postgres".into(),
            password: "postgres".into(),
            database_name: "seatunnel".into(),
            schema_name: "public".into(),
            table_name: "users".into(),
            split_column: "id".into(),
            publication_name: "seatunnel_pub".into(),
            slot_name: "seatunnel_slot".into(),
            auto_create_slot: true,
            startup_mode: PostgresStartupMode::Initial,
            parallelism: 1,
            subtask_index: 0,
            subtask_count: 1,
            schema_evolution: seatunnel_connector_cdc_base::SchemaEvolutionConfig::default(),
            decoding_plugin: "pgoutput".to_string(),
            snapshot_split_size: 8096,
            snapshot_fetch_size: 1024,
            table_matcher: PgTableMatcher::legacy("public", "users"),
            compat_warnings: Vec::new(),
        }
    }

    #[test]
    fn test_lsn_operations() {
        let lsn = Lsn::new(0x12345678, 0xABCDEF01);
        assert_eq!(lsn.high(), 0x12345678);
        assert_eq!(lsn.low(), 0xABCDEF01);
        let hex = lsn.fmt_hex();
        assert_eq!(hex, "12345678/ABCDEF01");
        let parsed = Lsn::from_hex(&hex).unwrap();
        assert_eq!(parsed.datum, lsn.datum);
        // Lowercase + short forms parse too.
        assert_eq!(Lsn::from_hex("0/16b6a70").unwrap().datum, 0x16B6A70);
    }

    #[test]
    fn test_lsn_ordering() {
        let lsn1 = Lsn::new(1, 0);
        let lsn2 = Lsn::new(1, 100);
        let lsn3 = Lsn::new(2, 0);
        assert!(lsn1 < lsn2);
        assert!(lsn2 < lsn3);
    }

    #[test]
    fn test_postgres_config_parsing() {
        let mut props = HashMap::new();
        props.insert("hostname".to_string(), "pg-host".to_string());
        props.insert("port".to_string(), "5433".to_string());
        props.insert("database-name".to_string(), "mydb".to_string());
        props.insert("schema-name".to_string(), "myschema".to_string());
        props.insert("table-name".to_string(), "users".to_string());
        props.insert("publication-name".to_string(), "my_pub".to_string());
        props.insert("slot-name".to_string(), "my_slot".to_string());
        props.insert("auto-create-slot".to_string(), "false".to_string());
        props.insert("split.column".to_string(), "order_id".to_string());
        let config = ConnectorConfig::new(props);
        let pg_config = PostgresCdcConfig::from_config(&config);
        assert_eq!(pg_config.hostname, "pg-host");
        assert_eq!(pg_config.port, 5433);
        assert_eq!(pg_config.database_name, "mydb");
        assert_eq!(pg_config.schema_name, "myschema");
        assert_eq!(pg_config.publication_name, "my_pub");
        assert_eq!(pg_config.slot_name, "my_slot");
        assert!(!pg_config.auto_create_slot);
        assert_eq!(pg_config.split_column, "order_id");
        assert_eq!(pg_config.qualified_table(), "\"myschema\".\"users\"");
    }

    #[test]
    fn test_postgres_connection_string() {
        let config = PostgresCdcConfig {
            hostname: "pg-host".to_string(),
            port: 5432,
            username: "user".to_string(),
            password: "pass".to_string(),
            database_name: "mydb".to_string(),
            ..PostgresCdcConfig::default()
        };
        let conn_str = config.connection_string();
        assert!(conn_str.contains("pg-host"));
        assert!(conn_str.contains("5432"));
        assert!(conn_str.contains("mydb"));
    }

    #[tokio::test]
    async fn open_fails_without_database() {
        // No synthetic fallbacks: an unreachable database must fail the task.
        let mut reader = PostgresCdcReader::new(test_config(), None);
        let result = reader.open().await;
        assert!(result.is_err(), "expected error, got {:?}", result);
    }

    #[tokio::test]
    async fn incremental_restore_skips_snapshot() {
        let mut reader = PostgresCdcReader::new(test_config(), None);
        reader.pending_ranges.push_back((0, 100));

        let mut offset = HashMap::new();
        offset.insert("lsn".to_string(), "0/16B6A70".to_string());
        let state = PostgresCdcState {
            phase: CdcPhase::Incremental,
            lsn: Lsn::from_hex("0/16B6A70").unwrap(),
            watermark: Watermark::Min,
            offset,
        };
        reader.apply_state(state);
        assert_eq!(reader.phase, CdcPhase::Incremental);
        assert!(reader.pending_ranges.is_empty());
        assert_eq!(reader.lsn.fmt_hex(), "00000000/016B6A70");
    }

    #[tokio::test]
    async fn state_roundtrip_preserves_progress() {
        let mut reader = PostgresCdcReader::new(test_config(), None);
        reader.pending_ranges.push_back((100, 200));
        reader.last_pk = 150;
        reader.lsn = Lsn::from_hex("1/AB").unwrap();
        reader.columns = vec!["id".into(), "name".into()];
        let bytes = reader.snapshot_state().await.unwrap();

        let mut restored = PostgresCdcReader::new(test_config(), None);
        restored.restore_from_state_bytes(&bytes).unwrap();
        assert_eq!(restored.last_pk, 150);
        assert_eq!(restored.pending_ranges.front(), Some(&(100, 200)));
        assert_eq!(restored.columns, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn pg_official_options_parsing() {
        let mk = |pairs: &[(&str, &str)]| {
            let props: std::collections::HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            PostgresCdcConfig::from_config(&ConnectorConfig::new(props))
        };
        // url connection form.
        let cfg = mk(&[("url", "jdbc:postgresql://pg.host:5433/inventory")]);
        assert_eq!(cfg.hostname, "pg.host");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.database_name, "inventory");

        // slot.name alias + decoding plugin + snapshot sizes.
        let cfg = mk(&[
            ("slot.name", "my_slot"),
            ("snapshot.split.size", "4096"),
            ("snapshot.fetch.size", "128"),
        ]);
        assert_eq!(cfg.slot_name, "my_slot");
        assert_eq!(cfg.snapshot_split_size, 4096);
        assert_eq!(cfg.snapshot_fetch_size, 128);

        // table-names entries (db.table / db.schema.table) + pattern.
        let cfg = mk(&[("table-names", "inventory.public.orders,analytics.events")]);
        assert!(cfg.table_matcher.matches("inventory", "public", "orders"));
        assert!(cfg.table_matcher.matches("analytics", "public", "events"));
        assert!(!cfg.table_matcher.matches("inventory", "public", "users"));

        let cfg = mk(&[("table-pattern", ".*\\.events_.*")]);
        assert!(
            cfg.table_matcher
                .matches("inventory", "public", "events_2026")
        );
        assert!(!cfg.table_matcher.matches("inventory", "public", "orders"));
    }

    #[test]
    fn absorb_event_uses_cached_column_order() {
        let mut reader = PostgresCdcReader::new(test_config(), None);
        reader.columns = vec!["id".into(), "name".into()];

        let payload = serde_json::json!({"name": "bob", "id": 7});
        let event = rustcdc::Event::builder("users", Operation::Insert)
            .after(payload)
            .source(rustcdc::SourceMetadata::new("postgres", "0/16B6A70", 1))
            .build();
        reader.absorb_event(event);

        let change = reader.stream_buffer.pop_front().expect("buffered row");
        assert_eq!(change.row.kind, RowKind::Insert);
        assert_eq!(change.row.field_count(), 2);
        // Column order follows the cached list, not JSON map ordering.
        assert_eq!(change.row.get(0), &Field::Int64(7));
        assert_eq!(change.row.get(1), &Field::String("bob".into()));
        assert_eq!(reader.lsn.datum, 0x16B6A70);
    }
}
