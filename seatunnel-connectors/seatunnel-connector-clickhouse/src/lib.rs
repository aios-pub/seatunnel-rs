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

//! ClickHouse connector (Java: `connector-clickhouse`) over the official
//! `clickhouse` crate (HTTP + RowBinary transport).
//!
//! ## Sink
//! Dynamic rows cannot use the crate's typed `insert<T>()` path, so rows
//! are serialized to `JSONEachRow` ourselves and streamed through
//! `insert_formatted_with` (the crate's pre-formatted insert channel).
//! Auto-created tables use `ReplacingMergeTree` ordered by `primary-keys`
//! when configured (idempotent replays) and plain `MergeTree` otherwise.
//! At-least-once: batches flush on size, linger, checkpoint and close.
//!
//! ## Source
//! Bounded reads driven by `toJSONString(tuple(*))` — the server serializes
//! each row (with column names) into a single String column that is
//! decoded into the crate's typed `Row` model, then mapped positionally
//! using the table's column order from `system.columns` / `DESCRIBE`.
//! Single-primary-key tables page with a `WHERE pk > last` cursor, so
//! checkpoint restore resumes exactly where the snapshot stopped.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use clickhouse::{Client, Row};
use seatunnel_api::row::{Row as SeatunnelRow, RowKind};
use seatunnel_api::schema::{ColumnDef, TableSchema};
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::sink::{Sink, SinkWriterContext};
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::source::source_split_enum::SourceSplitEnumeratorContext;
use seatunnel_api::source::{Boundedness, Source};
use seatunnel_api::{ColumnType, Field};
use seatunnel_connector_common::ConnectorConfig;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Startup behavior for the target table (Java `SchemaSaveMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChSchemaSaveMode {
    #[default]
    CreateWhenNotExist,
    ErrorWhenNotExist,
    RecreateSchema,
    Ignore,
}

/// Startup behavior for existing rows (Java `DataSaveMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChDataSaveMode {
    #[default]
    AppendData,
    DropData,
    ErrorWhenDataExists,
}

/// ClickHouse configuration (source + sink).
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// HTTP endpoint, e.g. `http://127.0.0.1:8123`.
    pub url: String,
    pub database: String,
    pub table: String,
    pub username: String,
    pub password: String,
    pub primary_keys: Vec<String>,
    pub max_batch_size: usize,
    pub max_retry_count: u32,
    /// Source: custom query instead of the table (must be deterministic;
    /// paging falls back to LIMIT/OFFSET).
    pub query: Option<String>,
    /// Source: page size.
    pub fetch_size: usize,
    /// Sink: explicit column names (positional `fN` otherwise, or schema
    /// field names when the engine propagates a schema).
    pub columns: Vec<String>,
    pub schema_save_mode: ChSchemaSaveMode,
    pub data_save_mode: ChDataSaveMode,
    pub batch_timeout_ms: u64,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        ClickHouseConfig {
            url: "http://127.0.0.1:8123".to_string(),
            database: "default".to_string(),
            table: "seatunnel".to_string(),
            username: "default".to_string(),
            password: String::new(),
            primary_keys: Vec::new(),
            max_batch_size: 1000,
            max_retry_count: 3,
            query: None,
            fetch_size: 1000,
            columns: Vec::new(),
            schema_save_mode: ChSchemaSaveMode::CreateWhenNotExist,
            data_save_mode: ChDataSaveMode::AppendData,
            batch_timeout_ms: 100,
        }
    }
}

impl ClickHouseConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let parse_schema_mode = |s: &str| match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "errorwhenschemanotexist" => ChSchemaSaveMode::ErrorWhenNotExist,
            "recreateschema" => ChSchemaSaveMode::RecreateSchema,
            "ignore" => ChSchemaSaveMode::Ignore,
            _ => ChSchemaSaveMode::CreateWhenNotExist,
        };
        let parse_data_mode = |s: &str| match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "dropdata" => ChDataSaveMode::DropData,
            "errorwhendataexists" => ChDataSaveMode::ErrorWhenDataExists,
            _ => ChDataSaveMode::AppendData,
        };
        ClickHouseConfig {
            url: {
                let url = config.get_string("url", &config.get_string("hosts", ""));
                if url.is_empty() {
                    "http://127.0.0.1:8123".to_string()
                } else if url.contains("://") {
                    url
                } else {
                    format!("http://{}", url)
                }
            },
            database: config.get_string("database", "default"),
            table: config.get_string("table", &config.get_string("table-name", "seatunnel")),
            username: config.get_string("username", "default"),
            password: config.get_string("password", ""),
            primary_keys: config
                .get_string("primary-keys", &config.get_string("primary_keys", ""))
                .split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect(),
            max_batch_size: config
                .get_int("max-batch-size", config.get_int("bulk-size", 1000))
                .max(1) as usize,
            max_retry_count: config.get_int("max-retry-count", 3).max(0) as u32,
            query: {
                let q = config.get_string("query", "");
                (!q.is_empty()).then_some(q)
            },
            fetch_size: config
                .get_int("fetch-size", config.get_int("fetch.size", 1000))
                .max(1) as usize,
            columns: config
                .get_string("columns", "")
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect(),
            schema_save_mode: parse_schema_mode(&config.get_string(
                "schema-save-mode",
                &config.get_string("schema_save_mode", ""),
            )),
            data_save_mode: parse_data_mode(
                &config.get_string("data-save-mode", &config.get_string("data_save_mode", "")),
            ),
            batch_timeout_ms: config.get_int("batch.timeout.ms", 100).max(0) as u64,
        }
    }

    fn client(&self) -> Client {
        Client::default()
            .with_url(&self.url)
            .with_user(&self.username)
            .with_password(&self.password)
            .with_database(&self.database)
    }

    /// Fully-qualified, backtick-quoted table identifier.
    pub fn qualified_table(&self) -> String {
        format!("`{}`.`{}`", self.database, self.table)
    }
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// ColumnType → ClickHouse DDL type.
pub fn ch_type(column_type: &ColumnType) -> String {
    match column_type {
        ColumnType::Bool => "UInt8".to_string(),
        ColumnType::Int8 => "Int8".to_string(),
        ColumnType::Int16 => "Int16".to_string(),
        ColumnType::Int32 => "Int32".to_string(),
        ColumnType::Int64 => "Int64".to_string(),
        ColumnType::UInt8 => "UInt8".to_string(),
        ColumnType::UInt16 => "UInt16".to_string(),
        ColumnType::UInt32 => "UInt32".to_string(),
        ColumnType::UInt64 => "UInt64".to_string(),
        ColumnType::Float32 => "Float32".to_string(),
        ColumnType::Float64 => "Float64".to_string(),
        ColumnType::Decimal { precision, scale } => {
            format!(
                "Decimal({}, {})",
                (*precision as i32).min(76),
                (*scale as i32).max(0)
            )
        }
        ColumnType::String => "String".to_string(),
        ColumnType::Bytes => "String".to_string(),
        ColumnType::Json => "String".to_string(),
        ColumnType::Date => "Date".to_string(),
        ColumnType::Time => "String".to_string(),
        ColumnType::DateTime => "DateTime".to_string(),
        ColumnType::TimestampTz => "DateTime64(3, 'UTC')".to_string(),
        ColumnType::Duration => "Int64".to_string(),
        ColumnType::Array { element_type } => format!("Array({})", ch_type(element_type)),
        ColumnType::Map { .. } => "String".to_string(),
        ColumnType::Nullable(inner) => format!("Nullable({})", ch_type(inner)),
    }
}

fn ch_type_of_field(field: &Field) -> ColumnType {
    match field {
        Field::Null => ColumnType::String,
        Field::Bool(_) => ColumnType::Bool,
        Field::Int8(_) => ColumnType::Int8,
        Field::Int16(_) => ColumnType::Int16,
        Field::Int32(_) => ColumnType::Int32,
        Field::Int64(_) => ColumnType::Int64,
        Field::UInt8(_) => ColumnType::UInt8,
        Field::UInt16(_) => ColumnType::UInt16,
        Field::UInt32(_) => ColumnType::UInt32,
        Field::UInt64(_) => ColumnType::UInt64,
        Field::Float32(_) => ColumnType::Float32,
        Field::Float64(_) => ColumnType::Float64,
        Field::Decimal(d) => {
            let scale = d.fractional_digit_count().max(0) as u32;
            ColumnType::Decimal {
                precision: ((d.digits() as i64) + (scale as i64)).clamp(1, 76) as u8,
                scale: scale.min(76) as i8,
            }
        }
        Field::String(_) => ColumnType::String,
        Field::Bytes(_) => ColumnType::Bytes,
        Field::Json(_) => ColumnType::Json,
        Field::Date(_) => ColumnType::Date,
        Field::Time(_) => ColumnType::Time,
        Field::DateTime(_) => ColumnType::DateTime,
        Field::TimestampTz(_) => ColumnType::TimestampTz,
        Field::Duration(_) => ColumnType::Duration,
        Field::Array(items) => ColumnType::Array {
            element_type: Box::new(
                items
                    .first()
                    .map(ch_type_of_field)
                    .unwrap_or(ColumnType::String),
            ),
        },
        Field::Row(_) => ColumnType::String,
    }
}

/// Field → JSONEachRow value. Bytes are hex-encoded and Decimals quoted
/// (both land in String columns); temporals use ClickHouse's
/// `YYYY-MM-DD hh:mm:ss` text form.
fn field_to_ch_json(field: &Field) -> serde_json::Value {
    match field {
        Field::Null => serde_json::Value::Null,
        Field::Bool(v) => serde_json::Value::Bool(*v),
        Field::Int8(v) => (*v as i64).into(),
        Field::Int16(v) => (*v as i64).into(),
        Field::Int32(v) => (*v).into(),
        Field::Int64(v) => (*v).into(),
        Field::UInt8(v) => (*v as u64).into(),
        Field::UInt16(v) => (*v as u64).into(),
        Field::UInt32(v) => (*v).into(),
        Field::UInt64(v) => (*v).into(),
        Field::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Field::Float64(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Field::Decimal(v) => serde_json::Value::String(v.to_string()),
        Field::String(v) => serde_json::Value::String(v.clone()),
        Field::Bytes(v) => serde_json::Value::String(hex::encode(v)),
        Field::Json(v) => v.clone(),
        Field::Date(v) => serde_json::Value::String(v.format("%Y-%m-%d").to_string()),
        Field::Time(v) => serde_json::Value::String(v.to_string()),
        Field::DateTime(v) => serde_json::Value::String(v.format("%Y-%m-%d %H:%M:%S").to_string()),
        Field::TimestampTz(v) => {
            serde_json::Value::String(v.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        }
        Field::Duration(v) => (*v).into(),
        Field::Array(v) => serde_json::Value::Array(v.iter().map(field_to_ch_json).collect()),
        Field::Row(v) => serde_json::Value::Array(v.iter().map(field_to_ch_json).collect()),
    }
}

fn ch_json_to_field(v: &serde_json::Value) -> Field {
    match v {
        serde_json::Value::Null => Field::Null,
        serde_json::Value::Bool(b) => Field::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Field::Int64(i)
            } else {
                Field::Float64(n.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(s) => Field::String(s.clone()),
        other => Field::Json(other.clone()),
    }
}

/// Row → JSONEachRow document with the given field names.
fn row_to_ch_object(row: &SeatunnelRow, field_names: Option<&[String]>) -> serde_json::Value {
    let mut doc = serde_json::Map::with_capacity(row.field_count());
    for (i, field) in row.fields.iter().enumerate() {
        let name = field_names
            .and_then(|names| names.get(i))
            .cloned()
            .unwrap_or_else(|| format!("f{i}"));
        doc.insert(name, field_to_ch_json(field));
    }
    serde_json::Value::Object(doc)
}

fn build_create_table_sql(table: &str, columns: &[ColumnDef], primary_keys: &[String]) -> String {
    let defs: Vec<String> = columns
        .iter()
        .map(|c| {
            format!(
                "`{}` {}",
                c.name.replace('`', "``"),
                ch_type(&c.column_type)
            )
        })
        .collect();
    let engine = if primary_keys.is_empty() {
        "MergeTree".to_string()
    } else {
        // ReplacingMergeTree deduplicates rows with equal sorting keys on
        // merge, making checkpoint replays idempotent.
        "ReplacingMergeTree()".to_string()
    };
    let order_by = if primary_keys.is_empty() {
        "tuple()".to_string()
    } else {
        format!(
            "({})",
            primary_keys
                .iter()
                .map(|k| format!("`{}`", k.replace('`', "``")))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "CREATE TABLE {} ({}) ENGINE = {} ORDER BY {}",
        table,
        defs.join(", "),
        engine,
        order_by
    )
}

// ---------------------------------------------------------------------------
// Typed single-column rows for the clickhouse crate
// ---------------------------------------------------------------------------

#[derive(Debug, Row, Deserialize)]
struct JsonStringRow {
    j: String,
}

#[derive(Debug, Row, Deserialize)]
struct CountRow {
    c: u64,
}

#[derive(Debug, Row, Deserialize)]
struct ExistsFlagRow {
    flag: u8,
}

#[derive(Debug, Row, Deserialize)]
struct NameRow {
    name: String,
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Opaque split handle.
#[derive(Debug, Clone)]
pub struct ClickHouseSplit {
    pub id: String,
}

impl SourceSplit for ClickHouseSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Bounded ClickHouse source reader.
pub struct ClickHouseSourceReader {
    config: ClickHouseConfig,
    /// Table column order, resolved at open (position anchor for rows).
    columns: Vec<String>,
    pending: VecDeque<SeatunnelRow>,
    /// Value of the primary key in the last emitted row (`pk > last`
    /// cursor) when exactly one primary key is configured.
    last_pk: Option<serde_json::Value>,
    /// Offset cursor for the LIMIT/OFFSET paging fallback.
    offset: u64,
    done: bool,
    opened: bool,
    total_emitted: u64,
}

impl ClickHouseSourceReader {
    pub fn new(config: ClickHouseConfig) -> Self {
        ClickHouseSourceReader {
            config,
            columns: Vec::new(),
            pending: VecDeque::new(),
            last_pk: None,
            offset: 0,
            done: false,
            opened: false,
            total_emitted: 0,
        }
    }

    /// Restore the paging cursor from a serialized `snapshot_state`.
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: serde_json::Value = serde_json::from_slice(bytes)?;
        self.last_pk = state.get("last_pk").filter(|v| !v.is_null()).cloned();
        self.offset = state.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
        self.done = state.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
        self.total_emitted = state
            .get("total_emitted")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        tracing::info!(
            "ClickHouse source: restored cursor (last_pk={:?}, offset={}, done={})",
            self.last_pk,
            self.offset,
            self.done
        );
        Ok(())
    }

    /// The query being paged: the configured `query` or the table.
    fn base_query(&self) -> String {
        match &self.config.query {
            Some(query) => format!("({query}) AS _seatunnel_src"),
            None => self.config.qualified_table(),
        }
    }

    /// Column order of the base relation.
    async fn resolve_columns(&self, client: &Client) -> anyhow::Result<Vec<String>> {
        if let Some(query) = &self.config.query {
            let sql = format!("DESCRIBE TABLE ({query})");
            let rows: Vec<NameRow> = client.query(&sql).fetch_all().await?;
            return Ok(rows.into_iter().map(|r| r.name).collect());
        }
        let sql = format!(
            "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            self.config.database.replace('\'', "''"),
            self.config.table.replace('\'', "''")
        );
        let rows: Vec<NameRow> = client.query(&sql).fetch_all().await?;
        Ok(rows.into_iter().map(|r| r.name).collect())
    }

    /// Page SQL: primary-key cursor when a single key column is
    /// configured, LIMIT/OFFSET otherwise.
    fn page_query(&self) -> anyhow::Result<String> {
        let base = self.base_query();
        let limit = self.config.fetch_size;
        if self.config.primary_keys.len() == 1 {
            let pk = &self.config.primary_keys[0];
            let predicate = match &self.last_pk {
                Some(value) => format!("`{}` > {}", pk.replace('`', "``"), sql_literal(value)),
                None => "1 = 1".to_string(),
            };
            Ok(format!(
                "SELECT toJSONString(tuple(*)) AS j FROM {base} WHERE {predicate} ORDER BY `{k}` ASC LIMIT {limit}",
                k = pk.replace('`', "``")
            ))
        } else if self.config.primary_keys.len() > 1 {
            anyhow::bail!("ClickHouse source supports at most one primary-key cursor column");
        } else {
            Ok(format!(
                "SELECT toJSONString(tuple(*)) AS j FROM {base} LIMIT {limit} OFFSET {offset}",
                offset = self.offset
            ))
        }
    }

    async fn fetch_page(&mut self, client: &Client) -> anyhow::Result<Vec<SeatunnelRow>> {
        let sql = self.page_query()?;
        let page: Vec<JsonStringRow> = client.query(&sql).fetch_all().await?;
        let columns = self.columns.clone();
        let pk_index = if self.config.primary_keys.len() == 1 {
            columns
                .iter()
                .position(|c| c == &self.config.primary_keys[0])
        } else {
            None
        };
        let mut rows = Vec::with_capacity(page.len());
        for record in page {
            let value: serde_json::Value = serde_json::from_str(&record.j)
                .map_err(|e| anyhow::anyhow!("server-side row JSON invalid: {}", e))?;
            let row = json_object_to_row(&value, &columns);
            if let Some(pk) = pk_index.and_then(|index| row.fields.get(index)) {
                self.last_pk = Some(field_to_ch_json(pk));
            }
            rows.push(row);
        }
        Ok(rows)
    }
}

/// Map a `toJSONString(tuple(*))` object to a positional row using the
/// table's column order. Objects with numeric keys (unnamed tuples) are
/// mapped positionally by key.
fn json_object_to_row(value: &serde_json::Value, columns: &[String]) -> SeatunnelRow {
    let map = match value {
        serde_json::Value::Object(map) => map,
        serde_json::Value::Array(items) => {
            let mut row = SeatunnelRow::new(RowKind::Insert, items.len());
            for (i, v) in items.iter().enumerate() {
                row.set(i, ch_json_to_field(v));
            }
            return row;
        }
        scalar => {
            let mut row = SeatunnelRow::new(RowKind::Insert, 1);
            row.set(0, ch_json_to_field(scalar));
            return row;
        }
    };
    let all_numeric = map.keys().all(|k| k.parse::<usize>().is_ok());
    let width = if all_numeric {
        map.len()
    } else {
        columns.len().max(map.len())
    };
    let mut row = SeatunnelRow::new(RowKind::Insert, width);
    if all_numeric {
        for (key, v) in map {
            if let Some(index) = key.parse::<usize>().ok().filter(|i| *i > 0) {
                row.set(index - 1, ch_json_to_field(v));
            }
        }
        return row;
    }
    for (i, name) in columns.iter().enumerate() {
        if let Some(v) = map.get(name) {
            row.set(i, ch_json_to_field(v));
        }
    }
    row
}

fn sql_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => other.to_string(),
    }
}

impl SourceReader for ClickHouseSourceReader {
    type Output = SeatunnelRow;
    type Split = ClickHouseSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if self.config.query.is_none() && self.config.table.is_empty() {
                anyhow::bail!("ClickHouse source requires 'table' or 'query'");
            }
            let client = self.config.client();
            self.columns = self.resolve_columns(&client).await?;
            if self.columns.is_empty() {
                anyhow::bail!("ClickHouse source: no columns resolved");
            }
            let rows = self.fetch_page(&client).await?;
            tracing::info!(
                "ClickHouse source: opened {} ({} columns, first page {} row(s))",
                self.config.qualified_table(),
                self.columns.len(),
                rows.len()
            );
            self.pending.extend(rows);
            self.opened = true;
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if let Some(row) = self.pending.pop_front() {
                self.total_emitted += 1;
                return Ok(PollResult::Record(row));
            }
            if !self.opened || self.done {
                return Ok(PollResult::EOF);
            }
            let client = self.config.client();
            let page = self.fetch_page(&client).await?;
            if page.len() < self.config.fetch_size {
                self.done = true;
            }
            self.offset += page.len() as u64;
            let mut iter = page.into_iter();
            match iter.next() {
                None => Ok(PollResult::EOF),
                Some(first) => {
                    self.pending.extend(iter);
                    self.total_emitted += 1;
                    Ok(PollResult::Record(first))
                }
            }
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = serde_json::json!({
            "last_pk": self.last_pk,
            "offset": self.offset,
            "done": self.done,
            "total_emitted": self.total_emitted,
        });
        Box::pin(async move { Ok(serde_json::to_vec(&state)?) })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }
}

/// ClickHouse source connector.
pub struct ClickHouseSource {
    pub config: ClickHouseConfig,
}

impl Source for ClickHouseSource {
    type Output = SeatunnelRow;
    type Split = ClickHouseSplit;
    type State = Vec<u8>;

    fn get_output_schema(&self) -> Option<TableSchema> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn enumerate_splits(
        &self,
        _context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        Ok(Vec::new())
    }

    fn create_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(ClickHouseSourceReader::new(self.config.clone())))
    }

    fn restore_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
        _state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(ClickHouseSourceReader::new(self.config.clone())))
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// ClickHouse sink writer: JSONEachRow batches via `insert_formatted_with`.
pub struct ClickHouseSinkWriter {
    config: ClickHouseConfig,
    field_names: Option<Vec<String>>,
    batch: Vec<SeatunnelRow>,
    total_written: usize,
    save_mode_applied: bool,
    last_flush: Instant,
}

impl ClickHouseSinkWriter {
    pub fn new(config: ClickHouseConfig, schema: Option<TableSchema>) -> Self {
        let field_names = if !config.columns.is_empty() {
            Some(config.columns.clone())
        } else {
            schema.map(|s| s.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>())
        };
        ClickHouseSinkWriter {
            config,
            field_names,
            batch: Vec::new(),
            total_written: 0,
            save_mode_applied: false,
            last_flush: Instant::now(),
        }
    }

    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: serde_json::Value = serde_json::from_slice(bytes)?;
        self.total_written = state
            .get("total_written")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        Ok(())
    }

    /// Resolve output columns for auto-created tables: explicit
    /// `columns` names (or schema names, or `fN`) with types inferred from
    /// the first sample row; without a sample everything degrades to
    /// String.
    fn columns_for(&self, sample: Option<&SeatunnelRow>) -> Vec<ColumnDef> {
        let names: Vec<String> = match &self.field_names {
            Some(names) if !names.is_empty() => names.clone(),
            _ => (0..sample.map(SeatunnelRow::field_count).unwrap_or(1))
                .map(|i| format!("f{i}"))
                .collect(),
        };
        names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let column_type = sample
                    .and_then(|row| row.fields.get(i))
                    .map(ch_type_of_field)
                    .unwrap_or(ColumnType::String);
                ColumnDef::new(name.clone(), column_type)
            })
            .collect()
    }

    async fn ensure_ready(&mut self, sample: Option<&SeatunnelRow>) -> anyhow::Result<()> {
        if self.save_mode_applied {
            return Ok(());
        }
        let client = self.config.client();
        let table = self.config.qualified_table();
        let exists: ExistsFlagRow = client
            .query(&format!(
                "SELECT count() > 0 AS flag FROM system.tables WHERE database = '{}' AND name = '{}'",
                self.config.database.replace('\'', "''"),
                self.config.table.replace('\'', "''")
            ))
            .fetch_one()
            .await?;
        let exists = exists.flag != 0;
        match self.config.schema_save_mode {
            ChSchemaSaveMode::ErrorWhenNotExist if !exists => {
                anyhow::bail!("table {} does not exist", table);
            }
            ChSchemaSaveMode::RecreateSchema => {
                if exists {
                    client
                        .query(&format!("DROP TABLE IF EXISTS {table}"))
                        .execute()
                        .await?;
                }
                let columns = self.columns_for(sample);
                client
                    .query(&build_create_table_sql(
                        &table,
                        &columns,
                        &self.config.primary_keys,
                    ))
                    .execute()
                    .await?;
                tracing::info!("ClickHouse sink: recreated table {}", table);
            }
            ChSchemaSaveMode::CreateWhenNotExist if !exists => {
                let columns = self.columns_for(sample);
                client
                    .query(&build_create_table_sql(
                        &table,
                        &columns,
                        &self.config.primary_keys,
                    ))
                    .execute()
                    .await?;
                tracing::info!("ClickHouse sink: auto-created table {}", table);
            }
            _ => {}
        }
        if exists || self.config.schema_save_mode == ChSchemaSaveMode::RecreateSchema {
            match self.config.data_save_mode {
                ChDataSaveMode::DropData => {
                    client
                        .query(&format!("TRUNCATE TABLE {table}"))
                        .execute()
                        .await?;
                }
                ChDataSaveMode::ErrorWhenDataExists => {
                    let count: CountRow = client
                        .query(&format!("SELECT count() AS c FROM {table}"))
                        .fetch_one()
                        .await?;
                    if count.c > 0 {
                        anyhow::bail!(
                            "table {} has {} row(s) and data-save-mode is error-when-data-exists",
                            table,
                            count.c
                        );
                    }
                }
                ChDataSaveMode::AppendData => {}
            }
        }
        self.save_mode_applied = true;
        Ok(())
    }

    async fn flush_batch(&mut self) -> anyhow::Result<usize> {
        self.last_flush = Instant::now();
        let rows = std::mem::take(&mut self.batch);
        if rows.is_empty() {
            return Ok(0);
        }
        if self.config.table.is_empty() {
            anyhow::bail!("ClickHouse sink requires a 'table' option");
        }
        let sample = rows.first().cloned();
        self.ensure_ready(sample.as_ref()).await?;

        let mut written = 0usize;
        let mut skipped = 0usize;
        for chunk in rows.chunks(self.config.max_batch_size) {
            let mut body = String::new();
            let mut chunk_rows = 0usize;
            for row in chunk {
                match row.kind {
                    // ClickHouse has no row-level deletes; ReplacingMergeTree
                    // upserts make INSERT/UPDATE_AFTER idempotent, DELETE is
                    // only expressible via async mutations and is skipped.
                    RowKind::Delete | RowKind::UpdateBefore => {
                        skipped += 1;
                        continue;
                    }
                    RowKind::Insert | RowKind::UpdateAfter => {}
                }
                body.push_str(&row_to_ch_object(row, self.field_names.as_deref()).to_string());
                body.push('\n');
                chunk_rows += 1;
            }
            if chunk_rows == 0 {
                continue;
            }
            let sql = format!(
                "INSERT INTO {} FORMAT JSONEachRow",
                self.config.qualified_table()
            );
            let client = self.config.client();
            let mut attempts = 0u32;
            loop {
                // A fresh statement per attempt: a failed INSERT is aborted
                // server-side and cannot be continued.
                let mut insert = client.insert_formatted_with(&sql);
                if let Err(e) = insert.send(bytes::Bytes::from(body.clone())).await {
                    return Err(anyhow::anyhow!("clickhouse insert send: {}", e));
                }
                match insert.end().await {
                    Ok(()) => break,
                    Err(e) if attempts < self.config.max_retry_count => {
                        attempts += 1;
                        tracing::warn!(
                            "ClickHouse insert failed (attempt {}/{}): {}",
                            attempts,
                            self.config.max_retry_count,
                            e
                        );
                        tokio::time::sleep(Duration::from_millis(300 * u64::from(attempts))).await;
                    }
                    Err(e) => return Err(anyhow::anyhow!("clickhouse insert: {}", e)),
                }
            }
            written += chunk_rows;
        }
        if skipped > 0 {
            tracing::warn!(
                "ClickHouse sink: skipped {} delete/update-before row(s) (not supported)",
                skipped
            );
        }
        self.total_written += written;
        Ok(written)
    }
}

impl SinkWriter for ClickHouseSinkWriter {
    type Input = SeatunnelRow;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Err(e) = self.ensure_ready(None).await {
                tracing::warn!(
                    "ClickHouse sink: table preparation deferred to first batch: {}",
                    e
                );
            }
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        self.batch.push(record);
        let full = self.batch.len() >= self.config.max_batch_size;
        let linger_due =
            self.last_flush.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms);
        Box::pin(async move {
            if full || linger_due {
                self.flush_batch().await?;
            }
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
        _checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            if self.save_mode_applied {
                self.flush_batch().await?;
            }
            Ok(vec![format!("written={}", self.total_written)])
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = serde_json::json!({
            "total_written": self.total_written,
            "pending": self.batch.len(),
        });
        Box::pin(async move { Ok(serde_json::to_vec(&state)?) })
    }

    fn poll_flush(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let due = !self.batch.is_empty()
            && self.last_flush.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms);
        Box::pin(async move {
            if due {
                self.flush_batch().await?;
            }
            Ok(())
        })
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if !self.batch.is_empty() {
                self.flush_batch().await?;
            }
            tracing::info!(
                "ClickHouse sink: closed, total written: {}",
                self.total_written
            );
            Ok(())
        })
    }
}

/// ClickHouse sink connector.
pub struct ClickHouseSink {
    pub config: ClickHouseConfig,
    pub schema: Option<TableSchema>,
}

impl Sink for ClickHouseSink {
    type Input = SeatunnelRow;
    type WriterState = Vec<u8>;
    type CommitInfo = String;
    type AggregatedCommitInfo = Vec<String>;

    fn create_writer(
        &self,
        _ctx: &SinkWriterContext,
    ) -> anyhow::Result<
        Box<
            dyn SinkWriter<
                    Input = Self::Input,
                    WriterState = Self::WriterState,
                    CommitInfo = Self::CommitInfo,
                >,
        >,
    > {
        Ok(Box::new(ClickHouseSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_writer(
        &self,
        _ctx: &SinkWriterContext,
        states: &[Vec<u8>],
    ) -> anyhow::Result<
        Box<
            dyn SinkWriter<
                    Input = Self::Input,
                    WriterState = Self::WriterState,
                    CommitInfo = Self::CommitInfo,
                >,
        >,
    > {
        let mut writer = ClickHouseSinkWriter::new(self.config.clone(), self.schema.clone());
        if let Some(bytes) = states.last() {
            let _ = writer.restore_from_state_bytes(bytes);
        }
        Ok(Box::new(writer))
    }

    fn get_input_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
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

    fn config_from(pairs: &[(&str, &str)]) -> ClickHouseConfig {
        let props: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ClickHouseConfig::from_config(&ConnectorConfig::new(props))
    }

    #[test]
    fn test_config_parsing() {
        let config = config_from(&[
            ("url", "http://ch:8123"),
            ("database", "analytics"),
            ("table", "events"),
            ("username", "u"),
            ("password", "p"),
            ("primary-keys", "id"),
            ("max-batch-size", "500"),
            ("schema-save-mode", "RECREATE_SCHEMA"),
            ("data-save-mode", "DROP_DATA"),
        ]);
        assert_eq!(config.url, "http://ch:8123");
        assert_eq!(config.database, "analytics");
        assert_eq!(config.table, "events");
        assert_eq!(config.primary_keys, vec!["id".to_string()]);
        assert_eq!(config.max_batch_size, 500);
        assert_eq!(config.schema_save_mode, ChSchemaSaveMode::RecreateSchema);
        assert_eq!(config.data_save_mode, ChDataSaveMode::DropData);
        assert_eq!(config.qualified_table(), "`analytics`.`events`");
    }

    #[test]
    fn test_url_scheme_injection() {
        let config = config_from(&[("url", "ch:8123")]);
        assert_eq!(config.url, "http://ch:8123");
    }

    #[test]
    fn test_ch_type_mapping() {
        assert_eq!(ch_type(&ColumnType::Int64), "Int64");
        assert_eq!(ch_type(&ColumnType::String), "String");
        assert_eq!(
            ch_type(&ColumnType::Decimal {
                precision: 10,
                scale: 2
            }),
            "Decimal(10, 2)"
        );
        assert_eq!(
            ch_type(&ColumnType::Array {
                element_type: Box::new(ColumnType::Int32)
            }),
            "Array(Int32)"
        );
        assert_eq!(
            ch_type(&ColumnType::Nullable(Box::new(ColumnType::String))),
            "Nullable(String)"
        );
        assert_eq!(ch_type(&ColumnType::TimestampTz), "DateTime64(3, 'UTC')");
    }

    #[test]
    fn test_create_table_sql() {
        let columns = vec![
            ColumnDef::new("id".to_string(), ColumnType::Int64),
            ColumnDef::new("name".to_string(), ColumnType::String),
        ];
        let sql = build_create_table_sql("`db`.`t`", &columns, &["id".to_string()]);
        assert_eq!(
            sql,
            "CREATE TABLE `db`.`t` (`id` Int64, `name` String) ENGINE = ReplacingMergeTree() ORDER BY (`id`)"
        );
        let sql = build_create_table_sql("`db`.`t`", &columns, &[]);
        assert!(sql.contains("ENGINE = MergeTree ORDER BY tuple()"));
    }

    #[test]
    fn test_row_to_ch_object() {
        let mut row = SeatunnelRow::new(RowKind::Insert, 2);
        row.set(0, Field::Int64(7));
        row.set(1, Field::String("a".into()));
        assert_eq!(
            row_to_ch_object(&row, None),
            serde_json::json!({"f0": 7, "f1": "a"})
        );
    }

    #[test]
    fn test_json_object_to_row_named_and_numeric() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let value = serde_json::json!({"id": 5, "name": "n"});
        let row = json_object_to_row(&value, &columns);
        assert_eq!(row.get(0), &Field::Int64(5));
        assert_eq!(row.get(1), &Field::String("n".into()));

        // Unnamed tuples serialize as {"1": .., "2": ..}.
        let value = serde_json::json!({"1": 5, "2": "n"});
        let row = json_object_to_row(&value, &columns);
        assert_eq!(row.get(0), &Field::Int64(5));
        assert_eq!(row.get(1), &Field::String("n".into()));
    }

    #[test]
    fn test_sql_literal() {
        assert_eq!(sql_literal(&serde_json::json!(42)), "42");
        assert_eq!(sql_literal(&serde_json::json!("o'brien")), "'o''brien'");
    }
}
