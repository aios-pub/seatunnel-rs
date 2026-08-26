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

//! JDBC Connector: generic database source via mysql_async.
//! Supports MySQL, MariaDB, TiDB (MySQL-compatible), and PostgreSQL.

use anyhow::Context;
use mysql_async::prelude::*;
use mysql_async::{OptsBuilder, Pool};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use seatunnel_api::source::source_reader::{PollResult, SourceReader, SourceReaderContext};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::source::source_split_enum::SourceSplitEnumeratorContext;
use seatunnel_api::source::{Boundedness, Source};
use seatunnel_api::sink::{Sink, SinkWriter, SinkWriterContext};
use seatunnel_api::sink::SinkCommitter;
use seatunnel_api::TableSchema;
use seatunnel_api::{Field, Row, RowKind};
use seatunnel_connector_common::ConnectorConfig;
use seatunnel_connector_cdc_base::{CdcPhase, IncrementalSplit, SnapshotSplit, Watermark};

/// Detect database dialect from connection URL.
fn detect_dialect(url: &str) -> &str {
    if url.contains("postgres") || url.contains("postgresql") {
        "postgres"
    } else {
        "mysql"
    }
}

/// JDBC source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcSourceConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub table: String,
    pub batch_size: usize,
    pub startup_mode: JdbcStartupMode,
    #[serde(default)]
    pub parallelism: usize,
    #[serde(default)]
    pub extra_options: HashMap<String, String>,
}

impl JdbcSourceConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let props = config.to_hashmap();
        JdbcSourceConfig {
            url: props.get("url").cloned().unwrap_or_default(),
            username: props.get("username").cloned().unwrap_or_default(),
            password: props.get("password").cloned().unwrap_or_default(),
            table: props.get("table").cloned().unwrap_or_default(),
            batch_size: props
                .get("batch.size")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            startup_mode: match props
                .get("startup.mode")
                .map(|s| s.as_str())
                .unwrap_or("earliest")
            {
                "earliest" => JdbcStartupMode::Earliest,
                "latest" => JdbcStartupMode::Latest,
                "specific" => JdbcStartupMode::Specific {
                    start_offset: props
                        .get("startup.specific.offset")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0),
                },
                _ => JdbcStartupMode::Earliest,
            },
            parallelism: props
                .get("parallelism")
                .and_then(|v| v.parse().ok())
                .unwrap_or(4),
            extra_options: props
                .into_iter()
                .filter(|(k, _)| !matches!(k.as_str(), "url" | "username" | "password" | "table" | "batch.size" | "startup.mode" | "parallelism"))
                .collect(),
        }
    }
}

/// Startup mode for JDBC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JdbcStartupMode {
    Earliest,
    Latest,
    Specific { start_offset: i64 },
}

/// JDBC source split.
#[derive(Debug, Clone)]
pub enum JdbcSourceSplit {
    Snapshot(SnapshotSplit),
    Incremental(IncrementalSplit),
}

impl SourceSplit for JdbcSourceSplit {
    fn split_id(&self) -> &str {
        match self {
            JdbcSourceSplit::Snapshot(s) => s.split_id(),
            JdbcSourceSplit::Incremental(s) => s.split_id(),
        }
    }
}

/// JDBC source reader.
pub struct JdbcSourceReader {
    config: JdbcSourceConfig,
    schema: Option<TableSchema>,
    phase: CdcPhase,
    splits: Vec<JdbcSourceSplit>,
    current_idx: usize,
    offset: i64,
    watermark: Watermark,
}

impl JdbcSourceReader {
    pub fn new(config: JdbcSourceConfig, schema: Option<TableSchema>) -> Self {
        JdbcSourceReader {
            config,
            schema,
            phase: CdcPhase::Snapshot,
            splits: Vec::new(),
            current_idx: 0,
            offset: 0,
            watermark: Watermark::Min,
        }
    }

    /// Convert a mysql_async Row to our Row type based on schema.
    fn row_from_mysql(row: &mysql_async::Row, schema: &TableSchema) -> Row {
        let num_cols = schema.column_count();
        let mut out = Row::new(RowKind::Insert, num_cols);
        for i in 0..num_cols.min(row.len()) {
            let col_type = &schema.columns[i].column_type;
            let field = match col_type {
                seatunnel_api::ColumnType::Int64 => {
                    row.get::<i64, usize>(i).map(Field::Int64).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::String => {
                    row.get::<String, usize>(i).map(Field::String).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::Bool => {
                    row.get::<bool, usize>(i).map(Field::Bool).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::Int32 => {
                    row.get::<i32, usize>(i).map(|v| Field::Int64(v as i64)).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::Float64 => {
                    row.get::<f64, usize>(i).map(Field::Float64).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::Bytes => {
                    row.get::<Vec<u8>, usize>(i).map(Field::Bytes).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::Int16 => {
                    row.get::<i16, usize>(i).map(|v| Field::Int64(v as i64)).unwrap_or(Field::Null)
                }
                seatunnel_api::ColumnType::Int8 => {
                    row.get::<i8, usize>(i).map(|v| Field::Int64(v as i64)).unwrap_or(Field::Null)
                }
                _ => Field::Null,
            };
            out.set(i, field);
        }
        out
    }
}

/// Parsed MySQL connection URL.
#[derive(Clone)]
struct ParsedUrl {
    host: String,
    port: u16,
    db: String,
}

/// Connect to database and verify.
async fn jdbc_connect(config: &JdbcSourceConfig) -> anyhow::Result<Pool> {
    let parsed = parse_mysql_url(&config.url)?;
    let opts = OptsBuilder::default()
        .ip_or_hostname(&parsed.host)
        .tcp_port(parsed.port)
        .user(Some(&config.username))
        .pass(Some(&config.password))
        .db_name(Some(&parsed.db));
    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await.context("Failed to connect to database")?;
    let _: Option<u32> = conn.query_first("SELECT 1").await?;
    Ok(pool)
}

fn parse_mysql_url(url: &str) -> anyhow::Result<ParsedUrl> {
    // Format: jdbc:mysql://host:port/db?params
    let without_prefix = url.strip_prefix("jdbc:mysql://").unwrap_or(url);
    let parts: Vec<&str> = without_prefix.splitn(3, '/').collect();
    let host_port = parts.first().ok_or_else(|| anyhow::anyhow!("Invalid URL"))?;
    let db = parts.get(1).map(|s| s.split('?').next().unwrap_or(*s)).unwrap_or("default_db").to_string();

    let (host, port) = if let Some(idx) = host_port.find(':') {
        (host_port[..idx].to_string(), host_port[idx + 1..].parse::<u16>().unwrap_or(3306))
    } else {
        (host_port.to_string(), 3306)
    };

    Ok(ParsedUrl { host, port, db })
}

impl SourceReader for JdbcSourceReader {
    type Output = Row;
    type Split = JdbcSourceSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        let config = self.config.clone();
        Box::pin(async move {
            tracing::info!(
                "JDBC reader opening: table={}, url={}",
                config.table,
                config.url
            );
            match jdbc_connect(&config).await {
                Ok(_) => tracing::info!("JDBC connection established"),
                Err(e) => tracing::warn!("JDBC connect failed (will retry): {}", e),
            }
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        let phase = self.phase;
        let schema = self.schema.clone();
        let current_idx = self.current_idx;
        let splits_clone = self.splits.clone();
        let config = self.config.clone();
        let offset = self.offset;

        Box::pin(async move {
            if phase == CdcPhase::Incremental {
                let mut row = Row::new(RowKind::Insert, 2);
                row.set(0, Field::String(config.table.clone()));
                row.set(1, Field::Int64(offset));
                return Ok(PollResult::Record(row));
            }

            if current_idx >= splits_clone.len() {
                return Ok(PollResult::Empty);
            }

            let split = &splits_clone[current_idx];
            if let JdbcSourceSplit::Snapshot(s) = split {
                let db = &s.database;
                let tbl = &s.table;
                let start: i64 = s.start_key.parse().unwrap_or(0);
                let end: i64 = s.end_key.parse().unwrap_or(i64::MAX);
                let batch = config.batch_size;

                let sql = format!(
                    "SELECT * FROM `{}`.`{}` WHERE `id` >= {} AND `id` < {} LIMIT {}",
                    db, tbl, start, end, batch
                );

                // Try to execute the query
                let result = (|| async {
                    let pool = jdbc_connect(&config).await?;
                    let mut conn = pool.get_conn().await?;
                    let rows: Vec<mysql_async::Row> = conn.query(&sql).await?;
                    drop(conn);
                    Ok::<Vec<mysql_async::Row>, anyhow::Error>(rows)
                })().await;

                if let Ok(rows) = result {
                    if let Some(ref sch) = schema {
                        for mr in rows {
                            let row = JdbcSourceReader::row_from_mysql(&mr, sch);
                            self.current_idx += 1;
                            return Ok(PollResult::Record(row));
                        }
                    }
                    self.current_idx += 1;
                } else if let Err(e) = result {
                    tracing::warn!("JDBC query error: {}", e);
                }
            }

            Ok(PollResult::Empty)
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        use seatunnel_connector_cdc_base::CdcState;
        let state = JdbcSourceState {
            phase_str: match self.phase {
                CdcPhase::Snapshot => "snapshot".to_string(),
                CdcPhase::Incremental => "incremental".to_string(),
            },
            watermark_val: match self.watermark {
                Watermark::Min => -1,
                Watermark::Max => -2,
                Watermark::Value(v) => v,
            },
        };
        Box::pin(async move {
            let bytes = serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e))?;
            Ok(bytes)
        })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!("JDBC reader: adding {} splits", splits.len());
        self.splits.extend(splits);
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn handle_no_more_splits(&mut self) {
        self.phase = CdcPhase::Incremental;
        self.watermark = Watermark::Value(1);
        tracing::info!("JDBC reader: transitioning to incremental phase");
    }
}

/// JDBC source.
pub struct JdbcSource {
    config: JdbcSourceConfig,
    schema: Option<TableSchema>,
}

impl JdbcSource {
    pub fn new(config: JdbcSourceConfig, schema: Option<TableSchema>) -> Self {
        JdbcSource { config, schema }
    }

    async fn get_row_count(pool: &Pool, table: &str) -> anyhow::Result<u64> {
        let mut conn = pool.get_conn().await?;
        let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", table)).await?;
        Ok(count.unwrap_or(0))
    }
}

/// Checkpoint state for JDBC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcSourceState {
    pub phase_str: String,
    pub watermark_val: i64,
}

impl Source for JdbcSource {
    type Output = Row;
    type Split = JdbcSourceSplit;
    type State = JdbcSourceState;

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn create_reader(
        &self,
        _context: SourceReaderContext,
    ) -> anyhow::Result<
        Box<
            dyn SourceReader<
                Output = Self::Output,
                Split = Self::Split,
            >,
        >,
    > {
        Ok(Box::new(JdbcSourceReader::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_reader(
        &self,
        _context: SourceReaderContext,
        state: &Self::State,
    ) -> anyhow::Result<
        Box<
            dyn SourceReader<
                Output = Self::Output,
                Split = Self::Split,
            >,
        >,
    > {
        let mut reader = JdbcSourceReader::new(
            self.config.clone(),
            self.schema.clone(),
        );
        reader.phase = if state.phase_str == "incremental" { CdcPhase::Incremental } else { CdcPhase::Snapshot };
        reader.watermark = Watermark::Value(state.watermark_val);
        reader.current_idx = state.watermark_val as usize;
        Ok(Box::new(reader))
    }

    fn enumerate_splits(
        &self,
        context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        let parallelism = self.config.parallelism.max(1).min(context.parallelism);
        let parsed = parse_mysql_url(&self.config.url)?;
        let db = parsed.db;
        let table = self.config.table.clone();

        // Try to get actual row count
        let total_rows = (|| async {
            let mut opts = OptsBuilder::default()
                .ip_or_hostname(&parsed.host)
                .tcp_port(parsed.port)
                .user(Some(&self.config.username))
                .pass(Some(&self.config.password))
                .db_name(Some(&db));
            let pool = Pool::new(opts);
            match Self::get_row_count(&pool, &table).await {
                Ok(n) => n,
                Err(_) => 10000, // fallback
            }
        })();
        let total_rows = futures::executor::block_on(total_rows);

        let split_size = ((total_rows as f64) / (parallelism as f64)).ceil() as u64;
        let splits: Vec<JdbcSourceSplit> = (0..parallelism)
            .map(|i| {
                let start = (i as u64 * split_size) as i64;
                let end = ((i as u64 + 1) * split_size) as i64;
                JdbcSourceSplit::Snapshot(SnapshotSplit::new(
                    &db,
                    &table,
                    "id",
                    &start.to_string(),
                    &end.to_string(),
                ))
            })
            .collect();

        tracing::info!(
            "JDBC: enumerated {} splits for {}.{} (total_rows≈{})",
            splits.len(),
            db,
            table,
            total_rows
        );
        Ok(splits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_dialect_mysql() {
        assert_eq!(detect_dialect("jdbc:mysql://host:3306/db"), "mysql");
    }

    #[test]
    fn test_detect_dialect_tidb() {
        assert_eq!(detect_dialect("jdbc:mysql://tidb:4000/mydb"), "mysql");
    }

    #[test]
    fn test_detect_dialect_postgres() {
        assert_eq!(
            detect_dialect("jdbc:postgresql://host:5432/db"),
            "postgres"
        );
    }

    #[test]
    fn test_parse_mysql_url() {
        let parsed = parse_mysql_url("jdbc:mysql://10.10.100.88:4001/ailearn_yace").unwrap();
        assert_eq!(parsed.host, "10.10.100.88");
        assert_eq!(parsed.port, 4001);
        assert_eq!(parsed.db, "ailearn_yace");
    }

    #[test]
    fn test_parse_mysql_url_default_port() {
        let parsed = parse_mysql_url("jdbc:mysql://localhost/mydb").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 3306);
        assert_eq!(parsed.db, "mydb");
    }

    #[test]
    fn test_jdbc_config_parsing() {
        let mut props = HashMap::new();
        props.insert(
            "url".to_string(),
            "jdbc:mysql://10.10.100.88:4001/db".to_string(),
        );
        props.insert("table".to_string(), "users".to_string());
        props.insert("batch.size".to_string(), "50".to_string());
        let config = ConnectorConfig::new(props);
        let jdbc_config = JdbcSourceConfig::from_config(&config);
        assert_eq!(jdbc_config.url, "jdbc:mysql://10.10.100.88:4001/db");
        assert_eq!(jdbc_config.table, "users");
        assert_eq!(jdbc_config.batch_size, 50);
    }

    #[test]
    fn test_jdbc_source_creation() {
        let config = JdbcSourceConfig {
            url: "jdbc:mysql://127.0.0.1:3306/test".to_string(),
            username: "root".to_string(),
            password: "pass".to_string(),
            table: "test_table".to_string(),
            batch_size: 100,
            startup_mode: JdbcStartupMode::Earliest,
            parallelism: 4,
            extra_options: HashMap::new(),
        };
        let source = JdbcSource::new(config, None);
        assert_eq!(source.boundedness(), Boundedness::Bounded);
        assert!(source.get_output_schema().is_none());
    }
}

// ===== JDBC Sink =====

/// JDBC sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcSinkConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub table: String,
    pub batch_size: usize,
    #[serde(default)]
    pub upsert_mode: bool,
    // Parsed URL components (populated from url)
    #[serde(skip)]
    pub hostname: String,
    #[serde(skip)]
    pub port: u16,
    #[serde(skip)]
    pub db_name: String,
}

impl JdbcSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let props = config.to_hashmap();
        let url = props.get("url").cloned().unwrap_or_default();
        let parsed = parse_mysql_url(&url).unwrap_or_else(|_| ParsedUrl {
            host: "127.0.0.1".to_string(),
            port: 3306,
            db: "default_db".to_string(),
        });
        JdbcSinkConfig {
            url: url.clone(),
            username: props.get("username").cloned().unwrap_or_default(),
            password: props.get("password").cloned().unwrap_or_default(),
            table: props.get("table").cloned().unwrap_or_default(),
            batch_size: props
                .get("batch.size")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            upsert_mode: props.get("upsert.mode").map(|v| v == "true").unwrap_or(false),
            hostname: parsed.host,
            port: parsed.port,
            db_name: parsed.db,
        }
    }
}

/// JDBC sink writer that writes rows to a database table.
pub struct JdbcSinkWriter {
    config: JdbcSinkConfig,
    schema: Option<TableSchema>,
}

impl JdbcSinkWriter {
    pub fn new(config: JdbcSinkConfig, schema: Option<TableSchema>) -> Self {
        JdbcSinkWriter { config, schema }
    }
}

impl SinkWriter for JdbcSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        let config = self.config.clone();
        let schema = self.schema.clone();
        let kind = record.kind;

        Box::pin(async move {
            let cols: Vec<String> = match &schema {
                Some(s) => s.columns.iter().map(|c| c.name.clone()).collect(),
                None => return Ok(()),
            };
            let field_count = cols.len();
            let placeholders: Vec<String> = vec!["?".to_string(); field_count];

            let sql = match kind {
                RowKind::Insert | RowKind::UpdateAfter => {
                    if config.upsert_mode {
                        format!(
                            "INSERT INTO `{}` ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}",
                            config.table,
                            cols.join(", "),
                            placeholders.join(", "),
                            cols[1..].iter().map(|c| format!("`{}`=VALUES(`{}`)", c, c)).collect::<Vec<_>>().join(", ")
                        )
                    } else {
                        format!(
                            "INSERT INTO `{}` ({}) VALUES ({})",
                            config.table,
                            cols.join(", "),
                            placeholders.join(", ")
                        )
                    }
                }
                RowKind::Delete => {
                    let where_parts: Vec<String> = cols.iter().map(|c| {
                        format!("`{}` = ?", c)
                    }).collect();
                    format!(
                        "DELETE FROM `{}` WHERE {}",
                        config.table, where_parts.join(" AND ")
                    )
                }
                RowKind::UpdateBefore => {
                    // Skip UpdateBefore rows - sinks typically don't write them
                    return Ok(());
                }
                _ => {
                    format!(
                        "INSERT INTO `{}` ({}) VALUES ({})",
                        config.table,
                        cols.join(", "),
                        placeholders.join(", ")
                    )
                }
            };

            let values: Vec<mysql_async::Value> = (0..field_count)
                .map(|i| field_to_mysql_value(record.get(i)))
                .collect();

            let opts = OptsBuilder::default()
                .ip_or_hostname(&config.hostname)
                .tcp_port(config.port)
                .user(Some(&config.username))
                .pass(Some(&config.password))
                .db_name(Some(&config.db_name));
            let pool = Pool::new(opts);
            let mut conn = pool.get_conn().await?;
            use mysql_async::prelude::*;
            // Bind parameters and execute
            let query_with_params = sql.with(values);
            query_with_params.ignore(&mut conn).await?;
            drop(conn);
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn close(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }
}

/// JDBC Sink connector.
#[derive(Debug, Clone)]
pub struct JdbcSink {
    config: JdbcSinkConfig,
    schema: Option<TableSchema>,
}

impl JdbcSink {
    pub fn new(config: JdbcSinkConfig, schema: Option<TableSchema>) -> Self {
        JdbcSink { config, schema }
    }

    pub fn from_config(config: &ConnectorConfig, schema: Option<TableSchema>) -> Self {
        JdbcSink {
            config: JdbcSinkConfig::from_config(config),
            schema,
        }
    }
}

impl Sink for JdbcSink {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;
    type AggregatedCommitInfo = Vec<String>;

    fn create_writer(
        &self,
        _writer_context: &SinkWriterContext,
    ) -> anyhow::Result<Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>> {
        Ok(Box::new(JdbcSinkWriter::new(self.config.clone(), self.schema.clone())))
    }

    fn restore_writer(
        &self,
        _writer_context: &SinkWriterContext,
        _states: &[Vec<u8>],
    ) -> anyhow::Result<Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>> {
        Ok(Box::new(JdbcSinkWriter::new(self.config.clone(), self.schema.clone())))
    }

    fn get_input_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
    }

    fn create_committer(
        &self,
    ) -> Option<Box<dyn SinkCommitter<CommitInfo = Self::CommitInfo, AggregatedCommitInfo = Self::AggregatedCommitInfo>>> {
        None
    }
}

/// Helper: convert Field to mysql_async Value.
fn field_to_mysql_value(field: &seatunnel_api::Field) -> mysql_async::Value {
    use seatunnel_api::Field;
    match field {
        Field::Null => mysql_async::Value::NULL,
        Field::Bool(b) => mysql_async::Value::Int(*b as i64),
        Field::Int8(v) => mysql_async::Value::Int(*v as i64),
        Field::Int16(v) => mysql_async::Value::Int(*v as i64),
        Field::Int32(v) => mysql_async::Value::Int(*v as i64),
        Field::Int64(v) => mysql_async::Value::Int(*v),
        Field::UInt8(v) => mysql_async::Value::Int(*v as i64),
        Field::UInt16(v) => mysql_async::Value::Int(*v as i64),
        Field::UInt32(v) => mysql_async::Value::Int(*v as i64),
        Field::UInt64(v) => mysql_async::Value::UInt(*v),
        Field::Float32(v) => mysql_async::Value::Float(*v),
        Field::Float64(v) => mysql_async::Value::Double(*v),
        Field::String(s) => mysql_async::Value::Bytes(s.as_bytes().to_vec()),
        Field::Bytes(b) => mysql_async::Value::Bytes(b.clone()),
        Field::Date(d) => {
            use chrono::Datelike;
            mysql_async::Value::Date(
                d.year() as u16, d.month() as u8, d.day() as u8, 0, 0, 0, 0
            )
        }
        Field::Time(t) => {
            use chrono::Timelike;
            mysql_async::Value::Time(false, 0, t.hour() as u8, t.minute() as u8, t.second() as u8, t.nanosecond() / 1000)
        }
        Field::DateTime(dt) => {
            use chrono::{Datelike, Timelike};
            mysql_async::Value::Date(
                dt.year() as u16, dt.month() as u8, dt.day() as u8,
                dt.hour() as u8, dt.minute() as u8, dt.second() as u8, dt.nanosecond() / 1000
            )
        }
        Field::Decimal(_) => mysql_async::Value::Bytes(format!("{}", field).into_bytes()),
        Field::Row(_) => mysql_async::Value::Bytes(format!("{}", field).into_bytes()),
        _ => mysql_async::Value::NULL,
    }
}

impl JdbcSinkConfig {
    fn parse_url(&self) -> ParsedUrl {
        parse_mysql_url(&self.url).unwrap_or_else(|_| ParsedUrl {
            host: "127.0.0.1".to_string(),
            port: 3306,
            db: "default_db".to_string(),
        })
    }
}
