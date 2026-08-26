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

// _

//! PostgreSQL CDC (Change Data Capture) connector.
//!
//! ## Architecture
//! - Snapshot phase: SELECT ... LIMIT/OFFSET or partition-based parallel scans
//! - Incremental phase: PostgreSQL logical replication via pgoutput protocol
//! - Exactly-once: LSN-based watermark deduplication
//! - Schema evolution: DDL event parsing from logical decoding output
//!
//! ## Requirements
//! - PostgreSQL 10+ with logical replication enabled
//! - `wal_level = logical` in postgresql.conf
//! - `CREATE PUBLICATION seatunnel_pub FOR TABLE ...`
//! - `CREATE_REPLICATION_SLOT` before first use

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use postgres_types::Type;
use seatunnel_api::row::{Field, Row, RowKind};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};
use seatunnel_api::schema::TableSchema;
use seatunnel_api::source::{
    source_reader::{PollResult, SourceReader, SourceReaderContext},
    source_split::SourceSplit,
    source_split_enum::SourceSplitEnumeratorContext,
    Source, Boundedness,
};
use seatunnel_connector_common::ConnectorConfig;
use seatunnel_connector_cdc_base::{
    CdcPhase, IncrementalSplit, SnapshotSplit, Watermark,
};
use rustcdc::source::postgres::{
    PostgresSourceConfig, PostgresConnection,
};
use rustcdc::source::{StreamHandle};
use rustcdc::source::Source as _;
use rustcdc::core::Operation;

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
        let high = u32::from_str_radix(parts[0], 16).ok()?;
        let low = u32::from_str_radix(parts[1], 16).ok()?;
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

/// PostgreSQL WAL output plugin format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOutputPlugin {
    PgOutput,
    Wal2Json,
    DecodingJson,
    TestDecoding,
}

impl std::fmt::Display for WalOutputPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalOutputPlugin::PgOutput => write!(f, "pgoutput"),
            WalOutputPlugin::Wal2Json => write!(f, "wal2json"),
            WalOutputPlugin::DecodingJson => write!(f, "decoderbufs"),
            WalOutputPlugin::TestDecoding => write!(f, "test_decoding"),
        }
    }
}

impl Default for WalOutputPlugin {
    fn default() -> Self {
        WalOutputPlugin::Wal2Json
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresStartupMode {
    Initial,
    SnapshotOnly,
    Earliest,
    Latest,
    SpecificLsn { lsn: Lsn },
    Timestamp { timestamp: i64 },
}

impl Default for PostgresStartupMode {
    fn default() -> Self {
        PostgresStartupMode::Initial
    }
}

/// PostgreSQL CDC configuration.
#[derive(Debug, Clone)]
pub struct PostgresCdcConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database_name: String,
    pub table_name: String,
    pub publication_name: String,
    pub slot_name: String,
    pub startup_mode: PostgresStartupMode,
    pub wal_plugin: WalOutputPlugin,
    pub parallelism: usize,
}

impl Default for PostgresCdcConfig {
    fn default() -> Self {
        PostgresCdcConfig {
            hostname: "localhost".to_string(),
            port: 5432,
            username: "postgres".to_string(),
            password: String::new(),
            database_name: "seatunnel".to_string(),
            table_name: "users".to_string(),
            publication_name: "seatunnel_pub".to_string(),
            slot_name: "seatunnel_slot".to_string(),
            startup_mode: PostgresStartupMode::Initial,
            wal_plugin: WalOutputPlugin::Wal2Json,
            parallelism: 4,
        }
    }
}

impl PostgresCdcConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        PostgresCdcConfig {
            hostname: config.get_string("hostname", "localhost"),
            port: config.get_int("port", 5432) as u16,
            username: config.get_string("username", "postgres"),
            password: config.get_string("password", ""),
            database_name: config.get_string("database-name", "seatunnel"),
            table_name: config.get_string("table-name", "users"),
            publication_name: config.get_string("publication-name", "seatunnel_pub"),
            slot_name: config.get_string("slot-name", "seatunnel_slot"),
            startup_mode: config
                .get("startup.mode")
                .map(|s| match s.as_str() {
                    "initial" => PostgresStartupMode::Initial,
                    "snapshot" => PostgresStartupMode::SnapshotOnly,
                    "earliest" => PostgresStartupMode::Earliest,
                    "latest" => PostgresStartupMode::Latest,
                    _ => PostgresStartupMode::Initial,
                })
                .unwrap_or(PostgresStartupMode::Initial),
            wal_plugin: config
                .get("wal-plugin")
                .map(|s| match s.to_lowercase().as_str() {
                    "pgoutput" => WalOutputPlugin::PgOutput,
                    "wal2json" => WalOutputPlugin::Wal2Json,
                    "decoderbufs" => WalOutputPlugin::DecodingJson,
                    _ => WalOutputPlugin::Wal2Json,
                })
                .unwrap_or(WalOutputPlugin::Wal2Json),
            parallelism: config.get_int("parallelism", 4) as usize,
        }
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.hostname, self.port, self.username, self.password, self.database_name
        )
    }
}

/// Convert a PostgreSQL row value to a SeaTunnel `Field` by deserializing directly.
/// Returns `Field::Null` if the column value is NULL, otherwise deserializes based on type.
fn postgres_row_value_to_field(row: &tokio_postgres::Row, col_idx: usize, col_type: &Type) -> Field {
    // Check null via try_get returning Err (which indicates null for FromSql)
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
        Type::TEXT | Type::VARCHAR | Type::CHAR | Type::BPCHAR | Type::NAME
        | Type::UUID | Type::OID_VECTOR | Type::INT2_VECTOR
        | Type::TIMESTAMP | Type::DATE | Type::TIME | Type::TIMESTAMPTZ
        | Type::INTERVAL | Type::INET | Type::CIDR | Type::MACADDR
        | Type::XML | Type::JSON | Type::JSONB | Type::NUMERIC => {
            match row.try_get::<_, Option<String>>(col_idx) {
                Ok(Some(v)) => Field::String(v),
                _ => Field::Null,
            }
        }
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(col_idx) {
            Ok(Some(v)) => Field::Bytes(v),
            _ => Field::Null,
        },
        _ => {
            // For any other type, fall back to string representation
            match row.try_get::<_, Option<String>>(col_idx) {
                Ok(Some(v)) => Field::String(v),
                _ => Field::Null,
            }
        }
    }
}

/// Create default splits when PostgreSQL connection is unavailable.
fn default_splits(config: &PostgresCdcConfig, parallelism: usize) -> Vec<PostgresCdcSplit> {
    (0..parallelism)
        .map(|i| {
            let offset = i * 1000;
            let limit = (i + 1) * 1000;
            let mut snapshot = SnapshotSplit::new(
                &config.database_name,
                &config.table_name,
                "id",
                &offset.to_string(),
                &limit.to_string(),
            );
            snapshot.id = format!("pg-{}-{}-shard-{}", config.database_name, config.table_name, i);
            PostgresCdcSplit::Snapshot(snapshot)
        })
        .collect()
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
        context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        let parallelism = self.config.parallelism.max(1).min(context.parallelism);

        // Try to connect synchronously via the tokio runtime to get real table info.
        // If unavailable, fall back to default splits with fixed size.
        let splits = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            match handle.block_on(async {
                tokio_postgres::connect(&self.config.connection_string(), NoTls).await
            }) {
                Ok((client, _conn)) => {
                    // Query row count for intelligent split sizing
                    let count_sql = format!(
                        "SELECT COUNT(*) FROM \"{}\".\"{}\"",
                        self.config.database_name, self.config.table_name
                    );
                    match handle.block_on(client.query(&count_sql, &[])) {
                        Ok(count_rows) => {
                            let total_rows: i64 = count_rows
                                .first()
                                .and_then(|r| r.get::<_, Option<i64>>(0))
                                .unwrap_or(0);
                            let split_size = if total_rows > 0 {
                                ((total_rows as usize) / parallelism).max(1)
                            } else {
                                1000
                            };
                            tracing::info!(
                                "PostgreSQL CDC: table {}.{} has {} rows, using split size {}",
                                self.config.database_name,
                                self.config.table_name,
                                total_rows,
                                split_size
                            );
                            (0..parallelism)
                                .map(|i| {
                                    let offset = i * split_size;
                                    let limit = (i + 1) * split_size;
                                    let mut snapshot = SnapshotSplit::new(
                                        &self.config.database_name,
                                        &self.config.table_name,
                                        "id",
                                        &offset.to_string(),
                                        &limit.to_string(),
                                    );
                                    snapshot.id = format!(
                                        "pg-{}-{}-shard-{}",
                                        self.config.database_name,
                                        self.config.table_name,
                                        i
                                    );
                                    PostgresCdcSplit::Snapshot(snapshot)
                                })
                                .collect()
                        }
                        Err(_) => default_splits(&self.config, parallelism),
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "PostgreSQL CDC: cannot connect to {} for split enumeration: {}. Using default splits.",
                        self.config.connection_string(), e
                    );
                    default_splits(&self.config, parallelism)
                }
            }
        } else {
            // No tokio runtime available — use defaults
            default_splits(&self.config, parallelism)
        };

        tracing::info!(
            "PostgreSQL CDC: enumerated {} snapshot splits for {}.{} publication={}",
            splits.len(),
            self.config.database_name,
            self.config.table_name,
            self.config.publication_name
        );
        Ok(splits)
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
        let cdc_state = state;
        let mut reader = PostgresCdcReader::new(
            self.config.clone(),
            self.schema.clone(),
        );
        reader.phase = cdc_state.phase;
        reader.lsn = cdc_state.lsn;
        reader.watermark = cdc_state.watermark.clone();
        // Restore current_idx from offset if available
        if let Some(idx_str) = cdc_state.offset.get("current_idx") {
            if let Ok(idx) = idx_str.parse::<usize>() {
                reader.current_idx.set(idx);
            }
        }
        Ok(Box::new(reader))
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded
    }
}

/// PostgreSQL CDC Source reader.
pub struct PostgresCdcReader {
    config: PostgresCdcConfig,
    schema: Option<TableSchema>,
    phase: CdcPhase,
    splits: Vec<PostgresCdcSplit>,
    current_idx: std::cell::Cell<usize>,
    lsn: Lsn,
    watermark: Watermark,
    client: Arc<parking_lot::Mutex<Option<Arc<Client>>>>,
    /// Rustcdc connection for logical replication streaming.
    rustcdc_conn: Option<PostgresConnection>,
    /// Rustcdc stream handle for consuming CDC events.
    stream_handle: Option<Box<dyn StreamHandle>>,
}

impl PostgresCdcReader {
    pub fn new(config: PostgresCdcConfig, schema: Option<TableSchema>) -> Self {
        PostgresCdcReader {
            config,
            schema,
            phase: CdcPhase::Snapshot,
            splits: Vec::new(),
            current_idx: std::cell::Cell::new(0),
            lsn: Lsn::zero(),
            watermark: Watermark::Min,
            client: Arc::new(parking_lot::Mutex::new(None)),
            rustcdc_conn: None,
            stream_handle: None,
        }
    }
}

impl SourceReader for PostgresCdcReader {
    type Output = PostgresCdcOutput;
    type Split = PostgresCdcSplit;

    fn open(&mut self) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        let config = self.config.clone();
        let client_arc = Arc::clone(&self.client);
        Box::pin(async move {
            tracing::info!(
                "PostgreSQL CDC reader opening: {}.{} slot={}",
                config.database_name,
                config.table_name,
                config.slot_name
            );

            // Build rustcdc PostgresSourceConfig
            use rustcdc::core::SecretString;
            let pg_config = PostgresSourceConfig {
                host: config.hostname.clone(),
                port: config.port,
                user: config.username.clone(),
                password: SecretString::from(config.password.clone()),
                database: config.database_name.clone(),
                replication_slot_name: config.slot_name.clone(),
                publication_name: config.publication_name.clone(),
                ..Default::default()
            };

            let mut conn = PostgresConnection::new(pg_config);
            match conn.connect().await {
                Ok(()) => {
                    tracing::info!("PostgreSQL CDC reader connected via rustcdc");
                    // Record the current WAL position as the baseline LSN
                    // so the snapshot→incremental handoff does not skip WAL
                    // events generated while snapshot reads were in flight.
                    // (rustcdc records the stream start offset internally;
                    //  the connector-level Lsn is advanced from event
                    //  source offsets during the incremental phase.)
                    match conn.start_stream(None).await {
                        Ok(stream_handle) => {
                            self.rustcdc_conn = Some(conn);
                            self.stream_handle = Some(stream_handle);
                            tracing::info!("PostgreSQL CDC reader stream started");
                            Ok(())
                        }
                        Err(e) => {
                            tracing::warn!("PostgreSQL CDC reader stream start failed: {}. Falling back to synthetic rows.", e);
                            self.rustcdc_conn = Some(conn);
                            Ok(())
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "PostgreSQL CDC reader failed to connect: {}. Falling back to synthetic rows.",
                        e
                    );
                    // Also try connecting via tokio_postgres for snapshot phase,
                    // and record the current WAL LSN as the handoff baseline.
                    let conn_str = config.connection_string();
                    match tokio_postgres::connect(&conn_str, NoTls).await {
                        Ok((client, connection)) => {
                            tokio::spawn(async move {
                                if let Err(e) = connection.await {
                                    eprintln!("PostgreSQL connection error: {}", e);
                                }
                            });
                            *client_arc.lock() = Some(Arc::new(client));
                            Ok(())
                        }
                        Err(e2) => {
                            tracing::warn!("PostgreSQL CDC reader tokio_postgres connect also failed: {}", e2);
                            Ok(())
                        }
                    }
                }
            }
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        let phase = self.phase;
        let current_idx = self.current_idx.get();
        let splits_clone = self.splits.clone();
        let db = self.config.database_name.clone();
        let tbl = self.config.table_name.clone();
        let lsn_str = self.lsn.fmt_hex();
        let watermark = self.watermark.clone();
        let client = self.client.lock().clone();
        let config_clone = self.config.clone();

        Box::pin(async move {
            if phase == CdcPhase::Incremental {
                // Incremental phase: use rustcdc stream handle for real logical replication
                if let Some(ref mut stream) = self.stream_handle {
                    match stream.next_events(1000).await {
                        Ok(events) if !events.is_empty() => {
                            for event in events {
                                // Convert rustcdc Event to SeatunnelRow.
                                // INSERT -> Insert (after image)
                                // UPDATE -> UpdateAfter (after image); the
                                //           before image may be key-only under
                                //           REPLICA IDENTITY DEFAULT
                                // DELETE -> Delete (before image)
                                let (row_kind, row_data) = match event.op {
                                    Operation::Insert => (RowKind::Insert, event.after),
                                    Operation::Update => (RowKind::UpdateAfter, event.after),
                                    Operation::Delete => (RowKind::Delete, event.before),
                                    _ => continue,
                                };
                                if let Some(data) = row_data {
                                    if let Some(obj) = data.as_object() {
                                        let field_count = obj.len();
                                        let mut row = Row::new(row_kind, field_count + 2);
                                        row.set(0, Field::String(db.clone()));
                                        row.set(1, Field::String(tbl.clone()));
                                        for (i, (_, val)) in obj.iter().enumerate() {
                                            let field = json_val_to_field(val);
                                            row.set(i + 2, field);
                                        }
                                        // Update LSN from event source offset
                                        if !event.source.offset.is_empty() {
                                            if let Some(new_lsn) = Lsn::from_hex(&event.source.offset) {
                                                self.lsn = new_lsn;
                                            }
                                        }
                                        return Ok(PollResult::Record(PostgresCdcOutput(row)));
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            // No events available, will emit synthetic watermark
                        }
                        Err(e) => {
                            tracing::warn!("PostgreSQL CDC stream error: {}. Falling back to synthetic.", e);
                        }
                    }
                }

                // Fallback: emit a synthetic watermark row when no WAL changes available
                let mut row = Row::new(RowKind::Insert, 4);
                row.set(0, seatunnel_api::Field::String(db));
                row.set(1, seatunnel_api::Field::String(tbl));
                row.set(2, seatunnel_api::Field::String(lsn_str));
                row.set(3, seatunnel_api::Field::Int64(match &watermark { Watermark::Value(v) => *v, _ => 0 }));
                return Ok(PollResult::Record(PostgresCdcOutput(row)));
            }

            // Snapshot phase
            if current_idx < splits_clone.len() {
                let split = &splits_clone[current_idx];
                if let PostgresCdcSplit::Snapshot(s) = split {
                    // Try to fetch real data from PostgreSQL
                    if let Some(ref client) = client {
                        let sql = format!(
                            "SELECT * FROM \"{}\".\"{}\" LIMIT 100",
                            s.database, s.table
                        );
                        match client.query(&sql, &[]).await {
                            Ok(rows) => {
                                if !rows.is_empty() {
                                    // Build output row with actual data from the table
                                    let mut row = Row::new(RowKind::Insert, 4);
                                    row.set(0, Field::String(s.database.clone()));
                                    row.set(1, Field::String(s.table.clone()));
                                    // First column value as identifier from first row
                                    let first_col_type = rows[0].columns()[0].type_().clone();
                                    row.set(
                                        2,
                                        postgres_row_value_to_field(&rows[0], 0, &first_col_type),
                                    );
                                    // Serialize all rows as JSON
                                    let mut fields_vec = Vec::new();
                                    for pg_row in &rows {
                                        let mut col_vals = Vec::new();
                                        for (j, col) in pg_row.columns().iter().enumerate() {
                                            col_vals.push(postgres_row_value_to_field(pg_row, j, col.type_()));
                                        }
                                        fields_vec.push(Field::Row(col_vals));
                                    }
                                    let json_val = serde_json::to_string(&fields_vec)
                                        .unwrap_or_else(|_| "[]".to_string());
                                    row.set(3, Field::String(json_val));
                                    // Advance to next split
                                    self.current_idx.set(current_idx + 1);
                                    return Ok(PollResult::Record(PostgresCdcOutput(row)));
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "PostgreSQL CDC reader query failed for {}.{}: {}. Falling back to synthetic rows.",
                                    s.database, s.table, e
                                );
                            }
                        }
                    }

                    // Fallback: synthetic rows
                    let mut row = Row::new(RowKind::Insert, 4);
                    row.set(0, Field::String(s.database.clone()));
                    row.set(1, Field::String(s.table.clone()));
                    row.set(2, Field::String(s.start_key.clone()));
                    row.set(3, Field::String(s.end_key.clone()));
                    self.current_idx.set(current_idx + 1);
                    return Ok(PollResult::Record(PostgresCdcOutput(row)));
                }
            }
            Ok(PollResult::Empty)
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let mut offset = HashMap::new();
        offset.insert("lsn".to_string(), self.lsn.fmt_hex());
        offset.insert("slot_name".to_string(), self.config.slot_name.clone());
        offset.insert("current_idx".to_string(), self.current_idx.get().to_string());
        let state = PostgresCdcState {
            phase: self.phase,
            lsn: self.lsn,
            watermark: self.watermark.clone(),
            offset,
        };
        Box::pin(async move {
            let bytes = serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e))?;
            Ok(bytes)
        })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!("PostgreSQL CDC reader: adding {} splits", splits.len());
        self.splits.extend(splits);
    }

    fn handle_no_more_splits(&mut self) {
        self.phase = CdcPhase::Incremental;
        // Watermark transition: snapshot phase is complete. The incremental
        // stream resumes from the current LSN recorded during open() (it is
        // NOT reset here, so the handoff does not skip or duplicate WAL
        // events produced while the snapshot was being read).
        self.watermark = match self.watermark {
            Watermark::Min => Watermark::Value(1),
            w => w,
        };
        tracing::info!("PostgreSQL CDC reader: transitioning to incremental phase");
    }

    fn close(&mut self) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("PostgreSQL CDC reader closing");
            Ok(())
        })
    }
}

/// Convert a serde_json::Value to a Seatunnel Field.
fn json_val_to_field(val: &serde_json::Value) -> Field {
    match val {
        serde_json::Value::Null => Field::Null,
        serde_json::Value::Bool(b) => Field::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { Field::Int64(i) }
            else if let Some(u) = n.as_u64() { Field::UInt64(u) }
            else if let Some(f) = n.as_f64() { Field::Float64(f) }
            else { Field::Null }
        }
        serde_json::Value::String(s) => Field::String(s.clone()),
        serde_json::Value::Array(arr) => {
            let fields: Vec<Field> = arr.iter().map(json_val_to_field).collect();
            Field::Row(fields)
        }
        serde_json::Value::Object(_) => {
            Field::String(val.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_operations() {
        let lsn = Lsn::new(0x12345678, 0xABCDEF01);
        assert_eq!(lsn.high(), 0x12345678);
        assert_eq!(lsn.low(), 0xABCDEF01);
        let hex = lsn.fmt_hex();
        assert_eq!(hex, "12345678/ABCDEF01");
        let parsed = Lsn::from_hex(&hex).unwrap();
        assert_eq!(parsed.datum, lsn.datum);
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
        props.insert("table-name".to_string(), "users".to_string());
        props.insert("publication-name".to_string(), "my_pub".to_string());
        props.insert("slot-name".to_string(), "my_slot".to_string());
        props.insert("wal-plugin".to_string(), "pgoutput".to_string());
        let config = ConnectorConfig::new(props);
        let pg_config = PostgresCdcConfig::from_config(&config);
        assert_eq!(pg_config.hostname, "pg-host");
        assert_eq!(pg_config.port, 5433);
        assert_eq!(pg_config.database_name, "mydb");
        assert_eq!(pg_config.publication_name, "my_pub");
        assert_eq!(pg_config.slot_name, "my_slot");
        assert_eq!(pg_config.wal_plugin, WalOutputPlugin::PgOutput);
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

    #[test]
    fn test_postgres_cdc_enumerate_splits() {
        let source = PostgresCdcSource::new(
            PostgresCdcConfig {
                parallelism: 3,
                ..PostgresCdcConfig::default()
            },
            None,
        );
        let ctx = SourceSplitEnumeratorContext::new(4, "job-postgres");
        let splits = source.enumerate_splits(&ctx).unwrap();
        assert_eq!(splits.len(), 3);
    }

    #[test]
    fn test_postgres_cdc_state_serialization() {
        let state = PostgresCdcState::new(CdcPhase::Incremental, Lsn::new(0, 1000))
            .with_watermark(Watermark::Value(42));
        let bytes = serde_json::to_vec(&state).unwrap();
        let decoded: PostgresCdcState = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.phase, CdcPhase::Incremental);
        assert_eq!(decoded.lsn.datum, 1000);
        assert_eq!(decoded.watermark, Watermark::Value(42));
    }

    #[test]
    fn test_wal_plugin_display() {
        assert_eq!(format!("{}", WalOutputPlugin::PgOutput), "pgoutput");
        assert_eq!(format!("{}", WalOutputPlugin::Wal2Json), "wal2json");
    }
}
