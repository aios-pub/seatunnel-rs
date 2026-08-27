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

//! Redis connector (Java: `connector-redis`).
//!
//! ## Source
//! `SCAN`-based bounded read over a key pattern; the read shape follows the
//! discovered key type (`TYPE`): STRING → one row per key, HASH → one row per
//! field, LIST/SET → one row per member, ZSET → one row per member+score.
//!
//! ## Sink
//! Batched pipeline writes per `data_type` (string/hash/list/set/zset) with
//! row-kind-aware deletes (DEL/HDEL/LREM/SREM/ZREM) and optional expiry.
//! Redis is schemaless, so schema change events only force a flush.

use std::future::Future;
use std::pin::Pin;

use redis::FromRedisValue;
use seatunnel_api::row::{Row, RowKind};
use seatunnel_api::schema::TableSchema;
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::sink::{Sink, SinkWriterContext};
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::source::{Boundedness, Source};
use seatunnel_api::{Field, SchemaChangeEvent};
use seatunnel_connector_common::ConnectorConfig;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Redis deployment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RedisMode {
    #[default]
    Single,
    Cluster,
}

/// Redis data type to write/read (Java `RedisDataType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RedisDataType {
    #[default]
    String,
    Hash,
    List,
    Set,
    ZSet,
}

impl RedisDataType {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "string" | "key" | "kv" => Ok(RedisDataType::String),
            "hash" | "map" => Ok(RedisDataType::Hash),
            "list" => Ok(RedisDataType::List),
            "set" => Ok(RedisDataType::Set),
            "zset" | "sortedset" | "sorted-set" => Ok(RedisDataType::ZSet),
            other => Err(anyhow::anyhow!("unknown redis data type '{}'", other)),
        }
    }
}

/// Shared Redis connection configuration.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_num: i64,
    pub mode: RedisMode,
    /// Cluster nodes (`host:port,...`), used in cluster mode.
    pub nodes: Vec<String>,
    /// Key pattern for the source (`SCAN MATCH`).
    pub keys_pattern: String,
    /// Sink key template; supports `${field}` placeholders (`f0`..`fN` for
    /// positional rows), default `${f0}`.
    pub key_template: String,
    /// Hash field template for HASH writes (default `${f1}`).
    pub hash_field_template: String,
    /// When set, only this row field is written as the value; otherwise the
    /// whole row is serialized by `format`.
    pub value_field: Option<String>,
    pub data_type: RedisDataType,
    /// Serialize rows as JSON (positional array) or delimiter-joined text.
    pub as_text: bool,
    pub field_delimiter: String,
    pub batch_size: usize,
    /// Key TTL seconds; `-1` disables.
    pub expire: i64,
    pub subtask_index: usize,
    pub subtask_count: usize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            host: "127.0.0.1".to_string(),
            port: 6379,
            user: String::new(),
            password: String::new(),
            db_num: 0,
            mode: RedisMode::Single,
            nodes: Vec::new(),
            keys_pattern: "*".to_string(),
            key_template: "${f0}".to_string(),
            hash_field_template: "${f1}".to_string(),
            value_field: None,
            data_type: RedisDataType::String,
            as_text: false,
            field_delimiter: ",".to_string(),
            batch_size: 10,
            expire: -1,
            subtask_index: 0,
            subtask_count: 1,
        }
    }
}

impl RedisConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let mode = if config
            .get_string("mode", "single")
            .eq_ignore_ascii_case("cluster")
        {
            RedisMode::Cluster
        } else {
            RedisMode::Single
        };
        let format = config.get_string("format", "json");
        RedisConfig {
            host: config.get_string("host", "127.0.0.1"),
            port: config.get_int("port", 6379) as u16,
            user: config.get_string("user", ""),
            password: config.get_string("auth", &config.get_string("password", "")),
            db_num: config.get_int("db-num", config.get_int("db_num", 0)),
            mode,
            nodes: config
                .get_string("nodes", "")
                .split(',')
                .map(|n| n.trim().to_string())
                .filter(|n| !n.is_empty())
                .collect(),
            keys_pattern: config.get_string("keys", "*"),
            key_template: config.get_string("key", "${f0}"),
            hash_field_template: config.get_string("hash-field", "${f1}"),
            value_field: {
                let v = config.get_string("value-field", "");
                if v.is_empty() { None } else { Some(v) }
            },
            data_type: RedisDataType::parse(&config.get_string("data-type", "string"))
                .unwrap_or(RedisDataType::String),
            as_text: format.eq_ignore_ascii_case("text"),
            field_delimiter: config.get_string("field-delimiter", ","),
            batch_size: config.get_int("batch-size", config.get_int("batch.size", 10)).max(1) as usize,
            expire: config.get_int("expire", -1),
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
        }
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

// One 240-byte connection per connector instance; boxing would only add
// an allocation.
#[allow(clippy::large_enum_variant)]
enum RedisConn {
    Single(redis::aio::ConnectionManager),
    Cluster(Box<redis::cluster_async::ClusterConnection>),
}

impl RedisConn {
    /// Execute one command against either connection flavor.
    async fn exec<T: FromRedisValue>(&mut self, cmd: &redis::Cmd) -> anyhow::Result<T> {
        let value = match self {
            RedisConn::Single(c) => cmd.query_async::<T>(c).await,
            RedisConn::Cluster(c) => cmd.query_async::<T>(c.as_mut()).await,
        }
        .map_err(|e| anyhow::anyhow!("redis command failed: {}", e))?;
        Ok(value)
    }

    /// Execute a pipeline against either connection flavor.
    async fn exec_pipe(&mut self, pipe: &redis::Pipeline) -> anyhow::Result<()> {
        match self {
            RedisConn::Single(c) => pipe.query_async::<()>(c).await,
            RedisConn::Cluster(c) => pipe.query_async::<()>(c.as_mut()).await,
        }
        .map_err(|e| anyhow::anyhow!("redis pipeline failed: {}", e))?;
        Ok(())
    }

    async fn connect(config: &RedisConfig) -> anyhow::Result<Self> {
        match config.mode {
            RedisMode::Single => {
                let mut url = if config.password.is_empty() {
                    format!("redis://{}:{}/{}", config.host, config.port, config.db_num)
                } else if config.user.is_empty() {
                    format!(
                        "redis://:{}@{}:{}/{}",
                        config.password, config.host, config.port, config.db_num
                    )
                } else {
                    format!(
                        "redis://{}:{}@{}:{}/{}",
                        config.user, config.password, config.host, config.port, config.db_num
                    )
                };
                // redis:// scheme does not carry the DB index when auth is
                // present in some server versions; select explicitly after.
                url = url.replace(&format!("/{}", config.db_num), "");
                let client = redis::Client::open(url)
                    .map_err(|e| anyhow::anyhow!("redis client: {}", e))?;
                let mut conn: redis::aio::ConnectionManager = client
                    .get_connection_manager()
                    .await
                    .map_err(|e| anyhow::anyhow!("redis connect: {}", e))?;
                if config.db_num > 0 {
                    let _: () = redis::cmd("SELECT")
                        .arg(config.db_num)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| anyhow::anyhow!("redis SELECT {}: {}", config.db_num, e))?;
                }
                Ok(RedisConn::Single(conn))
            }
            RedisMode::Cluster => {
                let node_addrs: Vec<String> = if config.nodes.is_empty() {
                    vec![format!("{}:{}", config.host, config.port)]
                } else {
                    config.nodes.clone()
                };
                let infos: Vec<redis::ConnectionInfo> = node_addrs
                    .iter()
                    .map(|n| {
                        let (host, port) = match n.rsplit_once(':') {
                            Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(6379)),
                            None => (n.clone(), 6379),
                        };
                        redis::ConnectionInfo {
                            addr: redis::ConnectionAddr::Tcp(host, port),
                            redis: redis::RedisConnectionInfo {
                                db: config.db_num,
                                username: if config.user.is_empty() {
                                    None
                                } else {
                                    Some(config.user.clone())
                                },
                                password: if config.password.is_empty() {
                                    None
                                } else {
                                    Some(config.password.clone())
                                },
                                ..Default::default()
                            },
                        }
                    })
                    .collect();
                let client = redis::cluster::ClusterClient::new(infos)
                    .map_err(|e| anyhow::anyhow!("redis cluster client: {}", e))?;
                let conn = client
                    .get_async_connection()
                    .await
                    .map_err(|e| anyhow::anyhow!("redis cluster connect: {}", e))?;
                Ok(RedisConn::Cluster(Box::new(conn)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Bounded Redis source: scans keys matching a pattern and materializes
/// one row per value / hash-field / member.
pub struct RedisSourceReader {
    config: RedisConfig,
    conn: Option<RedisConn>,
    scan_cursor: Option<u64>,
    scanned_keys: Vec<String>,
    key_index: usize,
    /// Rows materialized from the current key, awaiting emission.
    pending: std::collections::VecDeque<Row>,
    done: bool,
}

impl RedisSourceReader {
    pub fn new(config: RedisConfig) -> Self {
        RedisSourceReader {
            config,
            conn: None,
            scan_cursor: None,
            scanned_keys: Vec::new(),
            key_index: 0,
            pending: std::collections::VecDeque::new(),
            done: false,
        }
    }

    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.conn.is_none() {
            self.conn = Some(RedisConn::connect(&self.config).await?);
        }
        Ok(())
    }

    /// Advance the SCAN cursor; fills `scanned_keys`.
    async fn scan_batch(&mut self) -> anyhow::Result<bool> {
        let Some(conn) = &mut self.conn else {
            return Ok(false);
        };
        let (next_cursor, keys): (u64, Vec<String>) = conn
            .exec(redis::cmd("SCAN").cursor_arg(self.scan_cursor.unwrap_or(0)).arg("MATCH").arg(&self.config.keys_pattern).arg("COUNT").arg(self.config.batch_size))
            .await?;
        self.scanned_keys = keys;
        self.key_index = 0;
        if next_cursor == 0 {
            self.scan_cursor = None; // scan complete
            Ok(false)
        } else {
            self.scan_cursor = Some(next_cursor);
            Ok(true)
        }
    }

    /// Materialize rows for the key at `key_index`.
    async fn materialize_key(&mut self) -> anyhow::Result<()> {
        let key = match self.scanned_keys.get(self.key_index) {
            Some(k) => k.clone(),
            None => return Ok(()),
        };
        self.key_index += 1;
        let Some(conn) = &mut self.conn else {
            return Ok(());
        };
        let key_type: String = conn.exec(redis::cmd("TYPE").arg(&key)).await?;

        let mut rows = Vec::new();
        match key_type.as_str() {
            "string" => {
                let value: Option<String> = conn.exec(redis::cmd("GET").arg(&key)).await?;
                if let Some(v) = value {
                    rows.push(row_of(&[Field::String(key.clone()), Field::String(v)]));
                }
            }
            "hash" => {
                let fields: std::collections::HashMap<String, String> =
                    conn.exec(redis::cmd("HGETALL").arg(&key)).await?;
                let mut entries: Vec<_> = fields.into_iter().collect();
                entries.sort();
                for (f, v) in entries {
                    rows.push(row_of(&[
                        Field::String(key.clone()),
                        Field::String(f),
                        Field::String(v),
                    ]));
                }
            }
            "list" => {
                let members: Vec<String> = conn
                    .exec(redis::cmd("LRANGE").arg(&key).arg(0).arg(-1))
                    .await?;
                for m in members {
                    rows.push(row_of(&[Field::String(key.clone()), Field::String(m)]));
                }
            }
            "set" => {
                let mut members: Vec<String> =
                    conn.exec(redis::cmd("SMEMBERS").arg(&key)).await?;
                members.sort();
                for m in members {
                    rows.push(row_of(&[Field::String(key.clone()), Field::String(m)]));
                }
            }
            "zset" => {
                let pairs: Vec<(String, f64)> = conn
                    .exec(redis::cmd("ZRANGE").arg(&key).arg(0).arg(-1).arg("WITHSCORES"))
                    .await?;
                for (m, score) in pairs {
                    rows.push(row_of(&[
                        Field::String(key.clone()),
                        Field::String(m),
                        Field::Float64(score),
                    ]));
                }
            }
            other => {
                tracing::debug!("redis source: skipping key {} of type {}", key, other);
            }
        }
        self.pending.extend(rows);
        Ok(())
    }
}

fn row_of(fields: &[Field]) -> Row {
    let mut row = Row::new(RowKind::Insert, fields.len());
    for (i, f) in fields.iter().enumerate() {
        row.set(i, f.clone());
    }
    row
}

impl SourceReader for RedisSourceReader {
    type Output = Row;
    type Split = RedisSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_connected().await?;
            tracing::info!(
                "redis source: scanning pattern '{}' (mode={:?})",
                self.config.keys_pattern,
                self.config.mode
            );
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            loop {
                if let Some(row) = self.pending.pop_front() {
                    return Ok(PollResult::Record(row));
                }
                if self.done {
                    return Ok(PollResult::EOF);
                }
                if self.key_index < self.scanned_keys.len() {
                    self.materialize_key().await?;
                    continue;
                }
                let has_more = self.scan_batch().await?;
                if !has_more && self.scanned_keys.is_empty() {
                    self.done = true;
                    return Ok(PollResult::EOF);
                }
                if self.scanned_keys.is_empty() && !has_more {
                    self.done = true;
                    return Ok(PollResult::EOF);
                }
                if self.scanned_keys.is_empty() {
                    // cursor advanced but no keys matched in this batch
                    continue;
                }
            }
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = serde_json::json!({
            "cursor": self.scan_cursor,
            "key_index": self.key_index,
            "done": self.done,
        });
        Box::pin(async move { Ok(serde_json::to_vec(&state)?) })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        self.conn.take();
        Box::pin(async { Ok(()) })
    }
}

/// Opaque split handle (engine readers self-enumerate).
#[derive(Debug, Clone)]
pub struct RedisSplit {
    pub id: String,
}

impl SourceSplit for RedisSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Redis source connector.
pub struct RedisSource {
    pub config: RedisConfig,
}

impl Source for RedisSource {
    type Output = Row;
    type Split = RedisSplit;
    type State = Vec<u8>;

    fn get_output_schema(&self) -> Option<TableSchema> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn enumerate_splits(
        &self,
        _context: &seatunnel_api::source::source_split_enum::SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        // The reader scans inline; no pre-computed splits.
        Ok(Vec::new())
    }

    fn create_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(RedisSourceReader::new(self.config.clone())))
    }

    fn restore_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
        _state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(RedisSourceReader::new(self.config.clone())))
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// Resolve a `${field}` template against a row. Field selectors are `fN`
/// positional names (rows arrive without column names).
fn resolve_template(template: &str, row: &Row) -> String {
    let mut out = template.to_string();
    for i in 0..row.field_count() {
        let placeholder = format!("${{f{}}}", i);
        if out.contains(&placeholder) {
            let value = match row.get(i) {
                Field::String(s) => s.clone(),
                Field::Null => String::new(),
                other => format!("{}", other),
            };
            out = out.replace(&placeholder, &value);
        }
    }
    out
}

/// Serialize a row into the value string (whole row by format, or a single
/// configured field).
fn serialize_value(config: &RedisConfig, row: &Row) -> String {
    if let Some(field) = &config.value_field {
        if let Some(ordinal) = field.strip_prefix('f').and_then(|n| n.parse::<usize>().ok()) {
            return match row.fields.get(ordinal) {
                Some(Field::String(s)) => s.clone(),
                Some(Field::Null) | None => String::new(),
                Some(other) => format!("{}", other),
            };
        }
    }
    if config.as_text {
        row.fields
            .iter()
            .map(|f| match f {
                Field::String(s) => s.clone(),
                Field::Null => String::new(),
                other => format!("{}", other),
            })
            .collect::<Vec<_>>()
            .join(&config.field_delimiter)
    } else {
        serde_json::to_string(
            &row.fields
                .iter()
                .map(field_to_json)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default()
    }
}

fn field_to_json(field: &Field) -> serde_json::Value {
    match field {
        Field::Null => serde_json::Value::Null,
        Field::Bool(b) => serde_json::Value::Bool(*b),
        Field::Int8(v) => (*v as i64).into(),
        Field::Int16(v) => (*v as i64).into(),
        Field::Int32(v) => (*v as i64).into(),
        Field::Int64(v) => (*v).into(),
        Field::UInt8(v) => (*v as u64).into(),
        Field::UInt16(v) => (*v as u64).into(),
        Field::UInt32(v) => (*v as u64).into(),
        Field::UInt64(v) => (*v).into(),
        Field::Float32(v) => serde_json::Number::from_f64(*v as f64).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null),
        Field::Float64(v) => serde_json::Number::from_f64(*v).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null),
        Field::String(s) => serde_json::Value::String(s.clone()),
        Field::Bytes(b) => serde_json::Value::String(hex::encode(b)),
        Field::Decimal(d) => serde_json::Value::String(d.to_string()),
        Field::Json(j) => j.clone(),
        other => serde_json::Value::String(format!("{}", other)),
    }
}

/// Buffered Redis sink writing in pipelined batches.
pub struct RedisSinkWriter {
    config: RedisConfig,
    conn: Option<RedisConn>,
    buffer: Vec<Row>,
    written: u64,
}

impl RedisSinkWriter {
    pub fn new(config: RedisConfig) -> Self {
        RedisSinkWriter {
            config,
            conn: None,
            buffer: Vec::new(),
            written: 0,
        }
    }

    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.conn.is_none() {
            self.conn = Some(RedisConn::connect(&self.config).await?);
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_connected().await?;
        let rows = std::mem::take(&mut self.buffer);
        let mut pipe = redis::pipe();
        let config = &self.config;
        for row in &rows {
            let key = resolve_template(&config.key_template, row);
            let value = serialize_value(config, row);
            match row.kind {
                RowKind::Insert | RowKind::UpdateAfter | RowKind::UpdateBefore => {
                    match config.data_type {
                        RedisDataType::String => {
                            pipe.set(&key, value);
                        }
                        RedisDataType::Hash => {
                            let field = resolve_template(&config.hash_field_template, row);
                            pipe.hset(&key, field, value);
                        }
                        RedisDataType::List => {
                            pipe.lpush(&key, value);
                        }
                        RedisDataType::Set => {
                            pipe.sadd(&key, value);
                        }
                        RedisDataType::ZSet => {
                            // Java writes members with a fixed score of 1.
                            pipe.zadd(&key, value, 1.0f64);
                        }
                    }
                    if config.expire > 0 {
                        pipe.expire(&key, config.expire);
                    }
                }
                RowKind::Delete => {
                    match config.data_type {
                        RedisDataType::String => {
                            pipe.del(&key);
                        }
                        RedisDataType::Hash => {
                            let field = resolve_template(&config.hash_field_template, row);
                            pipe.hdel(&key, field);
                        }
                        RedisDataType::List => {
                            pipe.lrem(&key, 1, value);
                        }
                        RedisDataType::Set => {
                            pipe.srem(&key, value);
                        }
                        RedisDataType::ZSet => {
                            pipe.zrem(&key, value);
                        }
                    }
                }
            }
        }
        let written = rows.len() as u64;
        if let Some(conn) = &mut self.conn {
            conn.exec_pipe(&pipe).await?;
        } else {
            anyhow::bail!("redis sink not connected");
        }
        self.written += written;
        Ok(())
    }
}

impl SinkWriter for RedisSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Err(e) = self.ensure_connected().await {
                tracing::warn!("redis sink connection deferred: {}", e);
            }
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.buffer.push(record);
            if self.buffer.len() >= self.config.batch_size {
                self.flush().await?;
            }
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
        _checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            if self.conn.is_some() {
                self.flush().await?;
            }
            Ok(vec![format!("written={}", self.written)])
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let written = self.written;
        Box::pin(async move {
            Ok(serde_json::to_vec(&serde_json::json!({ "written": written }))?)
        })
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if self.conn.is_some() {
                self.flush().await?;
            }
            self.conn.take();
            Ok(())
        })
    }

    fn apply_schema_change(
        &mut self,
        _event: &SchemaChangeEvent,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Redis is schemaless: flush rows serialized with the old shape so
        // subsequent writes use the new field layout.
        Box::pin(async move {
            if self.conn.is_some() {
                self.flush().await?;
            }
            Ok(())
        })
    }
}

/// Redis sink connector.
pub struct RedisSink {
    pub config: RedisConfig,
}

impl Sink for RedisSink {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;
    type AggregatedCommitInfo = Vec<String>;

    fn create_writer(
        &self,
        _ctx: &SinkWriterContext,
    ) -> anyhow::Result<
        Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>,
    > {
        Ok(Box::new(RedisSinkWriter::new(self.config.clone())))
    }

    fn restore_writer(
        &self,
        _ctx: &SinkWriterContext,
        _states: &[Vec<u8>],
    ) -> anyhow::Result<
        Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>,
    > {
        Ok(Box::new(RedisSinkWriter::new(self.config.clone())))
    }

    fn get_input_schema(&self) -> Option<TableSchema> {
        None
    }

    fn create_committer(
        &self,
    ) -> Option<
        Box<
            dyn seatunnel_api::sink::SinkCommitter<
                CommitInfo = Self::CommitInfo,
                AggregatedCommitInfo = Self::AggregatedCommitInfo,
            >,
        >,
    > {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(fields: Vec<Field>) -> Row {
        row_of(&fields)
    }

    #[test]
    fn test_data_type_parsing() {
        assert_eq!(RedisDataType::parse("string").unwrap(), RedisDataType::String);
        assert_eq!(RedisDataType::parse("HASH").unwrap(), RedisDataType::Hash);
        assert_eq!(RedisDataType::parse("zset").unwrap(), RedisDataType::ZSet);
        assert!(RedisDataType::parse("bogus").is_err());
    }

    #[test]
    fn test_resolve_template() {
        let r = row(vec![
            Field::String("users:1".into()),
            Field::String("name".into()),
            Field::Int64(7),
        ]);
        assert_eq!(resolve_template("${f0}:${f2}", &r), "users:1:7");
        assert_eq!(resolve_template("literal", &r), "literal");
    }

    #[test]
    fn test_serialize_value() {
        let config = RedisConfig {
            as_text: true,
            field_delimiter: "|".into(),
            ..Default::default()
        };
        let r = row(vec![Field::Int64(1), Field::String("a".into()), Field::Null]);
        assert_eq!(serialize_value(&config, &r), "1|a|");

        let config = RedisConfig::default();
        assert_eq!(serialize_value(&config, &r), "[1,\"a\",null]");
    }

    #[test]
    fn test_serialize_single_field() {
        let config = RedisConfig {
            value_field: Some("f1".into()),
            ..Default::default()
        };
        let r = row(vec![Field::String("k".into()), Field::String("v".into())]);
        assert_eq!(serialize_value(&config, &r), "v");
    }

    #[test]
    fn test_config_parsing() {
        let props: std::collections::HashMap<String, String> = [
            ("host", "redis-host"),
            ("port", "6380"),
            ("auth", "secret"),
            ("keys", "user:*"),
            ("data-type", "hash"),
            ("batch-size", "50"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let config = RedisConfig::from_config(&ConnectorConfig::new(props));
        assert_eq!(config.host, "redis-host");
        assert_eq!(config.port, 6380);
        assert_eq!(config.password, "secret");
        assert_eq!(config.keys_pattern, "user:*");
        assert_eq!(config.data_type, RedisDataType::Hash);
        assert_eq!(config.batch_size, 50);
    }
}
