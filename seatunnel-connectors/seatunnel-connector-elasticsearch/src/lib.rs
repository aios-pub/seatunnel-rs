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

//! Elasticsearch connector (Java: `connector-elasticsearch`).
//!
//! ## Sink
//! Batched `_bulk` writes with `doc_as_upsert` upserts keyed by
//! `primary_keys` (joined with `key_delimiter`), delete support, index
//! auto-creation with explicit mappings (schema save mode), optional data
//! clearing (data save mode) and additive mapping updates on schema change.
//!
//! ## Source
//! Bounded scroll-based read (`_search?scroll=...` + `_search/scroll`).

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use reqwest::{Client, Method, StatusCode};
use seatunnel_api::row::{Row, RowKind};
use seatunnel_api::schema::{ColumnDef, TableSchema};
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::sink::{Sink, SinkWriterContext};
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::source::source_split_enum::SourceSplitEnumeratorContext;
use seatunnel_api::source::{Boundedness, Source};
use seatunnel_api::{ColumnType, Field, SchemaChangeEvent};
use seatunnel_connector_common::ConnectorConfig;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Startup behavior for the target index (Java `SchemaSaveMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EsSchemaSaveMode {
    #[default]
    CreateWhenNotExist,
    ErrorWhenNotExist,
    RecreateSchema,
    Ignore,
}

/// Startup behavior for existing documents (Java `DataSaveMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EsDataSaveMode {
    #[default]
    AppendData,
    DropData,
    ErrorWhenDataExists,
}

/// Elasticsearch configuration (source + sink).
#[derive(Debug, Clone)]
pub struct EsConfig {
    /// `host:port` entries (http/https prefixes allowed).
    pub hosts: Vec<String>,
    pub username: String,
    pub password: String,
    /// Index name; supports `${fN}` variable placeholders for the sink.
    pub index: String,
    pub primary_keys: Vec<String>,
    pub key_delimiter: String,
    pub batch_size: usize,
    pub max_retry_count: u32,
    pub scroll_time: String,
    pub scroll_size: usize,
    /// Source query DSL (JSON); defaults to match_all.
    pub query: Option<String>,
    pub schema_save_mode: EsSchemaSaveMode,
    pub data_save_mode: EsDataSaveMode,
}

impl Default for EsConfig {
    fn default() -> Self {
        EsConfig {
            hosts: vec!["127.0.0.1:9200".to_string()],
            username: String::new(),
            password: String::new(),
            index: "seatunnel".to_string(),
            primary_keys: Vec::new(),
            key_delimiter: "_".to_string(),
            batch_size: 100,
            max_retry_count: 3,
            scroll_time: "1m".to_string(),
            scroll_size: 100,
            query: None,
            schema_save_mode: EsSchemaSaveMode::CreateWhenNotExist,
            data_save_mode: EsDataSaveMode::AppendData,
        }
    }
}

impl EsConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let parse_schema_mode = |s: &str| match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "errorwhenschemanotexist" => EsSchemaSaveMode::ErrorWhenNotExist,
            "recreateschema" => EsSchemaSaveMode::RecreateSchema,
            "ignore" => EsSchemaSaveMode::Ignore,
            _ => EsSchemaSaveMode::CreateWhenNotExist,
        };
        let parse_data_mode = |s: &str| match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "dropdata" => EsDataSaveMode::DropData,
            "errorwhendataexists" => EsDataSaveMode::ErrorWhenDataExists,
            _ => EsDataSaveMode::AppendData,
        };
        EsConfig {
            hosts: config
                .get_string("hosts", "127.0.0.1:9200")
                .split(',')
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty())
                .map(|h| {
                    if h.contains("://") {
                        h
                    } else {
                        format!("http://{}", h)
                    }
                })
                .collect(),
            username: config.get_string("username", ""),
            password: config.get_string("password", ""),
            index: config.get_string("index", "seatunnel"),
            primary_keys: config
                .get_string("primary-keys", &config.get_string("primary_keys", ""))
                .split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect(),
            key_delimiter: config.get_string("key-delimiter", "_"),
            batch_size: config
                .get_int("max-batch-size", config.get_int("batch.size", 100))
                .max(1) as usize,
            max_retry_count: config.get_int("max-retry-count", 3).max(0) as u32,
            scroll_time: config.get_string("scroll-time", "1m"),
            scroll_size: config.get_int("scroll-size", 100).max(1) as usize,
            query: {
                let q = config.get_string("query", "");
                if q.is_empty() { None } else { Some(q) }
            },
            schema_save_mode: parse_schema_mode(&config.get_string(
                "schema-save-mode",
                &config.get_string("schema_save_mode", ""),
            )),
            data_save_mode: parse_data_mode(&config.get_string(
                "data-save-mode",
                &config.get_string("data_save_mode", ""),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// REST client
// ---------------------------------------------------------------------------

/// Thin Elasticsearch REST client over reqwest with basic auth.
pub struct EsClient {
    http: Client,
    hosts: Vec<String>,
    auth_header: Option<String>,
    host_index: std::sync::atomic::AtomicUsize,
}

impl EsClient {
    pub fn new(config: &EsConfig) -> anyhow::Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| anyhow::anyhow!("http client: {}", e))?;
        let auth_header = if !config.username.is_empty() {
            let raw = format!("{}:{}", config.username, config.password);
            Some(format!(
                "Basic {}",
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw)
            ))
        } else {
            None
        };
        Ok(EsClient {
            http,
            hosts: config.hosts.clone(),
            auth_header,
            host_index: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn next_host(&self) -> &str {
        let idx = self.host_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.hosts[idx % self.hosts.len()]
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<String>,
        content_type: &str,
    ) -> anyhow::Result<(StatusCode, String)> {
        let mut attempts = 0u32;
        loop {
            let host = self.next_host();
            let mut builder = self.http.request(method.clone(), format!("{}{}", host, path));
            if let Some(auth) = &self.auth_header {
                builder = builder.header("Authorization", auth);
            }
            if let Some(body) = &body {
                builder = builder
                    .header("Content-Type", content_type)
                    .body(body.clone());
            }
            let resp = builder.send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            // Retry server-side / rate-limit failures only.
            if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                && attempts < 2
            {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            return Ok((status, text));
        }
    }

    pub async fn index_exists(&self, index: &str) -> anyhow::Result<bool> {
        let (status, _) = self.request(Method::HEAD, &format!("/{}", index), None, "").await?;
        Ok(status == StatusCode::OK)
    }

    pub async fn create_index(&self, index: &str, mappings: serde_json::Value) -> anyhow::Result<()> {
        let body = serde_json::json!({ "mappings": mappings });
        let (status, text) = self
            .request(
                Method::PUT,
                &format!("/{}", index),
                Some(body.to_string()),
                "application/json",
            )
            .await?;
        if !status.is_success() {
            anyhow::bail!("create index {} failed ({}): {}", index, status, text);
        }
        Ok(())
    }

    pub async fn delete_index(&self, index: &str) -> anyhow::Result<()> {
        let (status, text) = self
            .request(Method::DELETE, &format!("/{}", index), None, "")
            .await?;
        if !status.is_success() && status != StatusCode::NOT_FOUND {
            anyhow::bail!("delete index {} failed ({}): {}", index, status, text);
        }
        Ok(())
    }

    pub async fn update_mapping(
        &self,
        index: &str,
        properties: serde_json::Value,
    ) -> anyhow::Result<()> {
        let body = serde_json::json!({ "properties": properties });
        let (status, text) = self
            .request(
                Method::PUT,
                &format!("/{}/_mapping", index),
                Some(body.to_string()),
                "application/json",
            )
            .await?;
        if !status.is_success() {
            anyhow::bail!("update mapping on {} failed ({}): {}", index, status, text);
        }
        Ok(())
    }

    pub async fn bulk(&self, ndjson: &str) -> anyhow::Result<()> {
        if ndjson.is_empty() {
            return Ok(());
        }
        let (status, text) = self
            .request(Method::POST, "/_bulk", Some(ndjson.to_string()), "application/x-ndjson")
            .await?;
        if !status.is_success() {
            anyhow::bail!("_bulk failed ({}): {}", status, text);
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
            if value.get("errors").and_then(|e| e.as_bool()).unwrap_or(false) {
                let first = value
                    .pointer("/items/0")
                    .map(|i| i.to_string())
                    .unwrap_or_default();
                anyhow::bail!("_bulk reported item-level errors: {}", first);
            }
        }
        Ok(())
    }

    pub async fn count(&self, index: &str) -> anyhow::Result<u64> {
        let (status, text) = self
            .request(Method::GET, &format!("/{}/_count", index), None, "")
            .await?;
        if !status.is_success() {
            anyhow::bail!("count on {} failed ({}): {}", index, status, text);
        }
        let value: serde_json::Value = serde_json::from_str(&text)?;
        Ok(value.get("count").and_then(|c| c.as_u64()).unwrap_or(0))
    }

    pub async fn delete_by_query(&self, index: &str) -> anyhow::Result<()> {
        let body = serde_json::json!({ "query": { "match_all": {} } });
        let (status, text) = self
            .request(
                Method::POST,
                &format!("/{}/_delete_by_query", index),
                Some(body.to_string()),
                "application/json",
            )
            .await?;
        if !status.is_success() {
            anyhow::bail!("delete_by_query on {} failed ({}): {}", index, status, text);
        }
        Ok(())
    }

    /// Start a scroll search; returns (scroll_id, hits).
    pub async fn search_open(
        &self,
        index: &str,
        query: Option<&str>,
        scroll_time: &str,
        size: usize,
    ) -> anyhow::Result<(Option<String>, Vec<serde_json::Value>)> {
        let query_dsl: serde_json::Value = match query {
            Some(q) => serde_json::from_str(q)
                .map_err(|e| anyhow::anyhow!("invalid query DSL: {}", e))?,
            None => serde_json::json!({ "match_all": {} }),
        };
        let body = serde_json::json!({ "size": size, "query": query_dsl });
        let (status, text) = self
            .request(
                Method::POST,
                &format!("/{}/_search?scroll={}", index, scroll_time),
                Some(body.to_string()),
                "application/json",
            )
            .await?;
        if !status.is_success() {
            anyhow::bail!("scroll search on {} failed ({}): {}", index, status, text);
        }
        let value: serde_json::Value = serde_json::from_str(&text)?;
        let scroll_id = value.get("_scroll_id").and_then(|s| s.as_str()).map(String::from);
        let hits = extract_hits(&value);
        Ok((scroll_id, hits))
    }

    pub async fn scroll_next(
        &self,
        scroll_id: &str,
        scroll_time: &str,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let body = serde_json::json!({ "scroll": scroll_time, "scroll_id": scroll_id });
        let (status, text) = self
            .request(
                Method::POST,
                "/_search/scroll",
                Some(body.to_string()),
                "application/json",
            )
            .await?;
        if !status.is_success() {
            anyhow::bail!("scroll continuation failed ({}): {}", status, text);
        }
        let value: serde_json::Value = serde_json::from_str(&text)?;
        Ok(extract_hits(&value))
    }

    pub async fn scroll_close(&self, scroll_id: &str) {
        let body = serde_json::json!({ "scroll_id": [scroll_id] });
        let _ = self
            .request(
                Method::DELETE,
                "/_search/scroll",
                Some(body.to_string()),
                "application/json",
            )
            .await;
    }
}

fn extract_hits(value: &serde_json::Value) -> Vec<serde_json::Value> {
    value
        .pointer("/hits/hits")
        .and_then(|h| h.as_array())
        .map(|hits| hits.to_vec())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Mapping conversion
// ---------------------------------------------------------------------------

/// ColumnType → Elasticsearch mapping property.
pub fn es_property(column_type: &ColumnType) -> serde_json::Value {
    match column_type {
        ColumnType::Bool => serde_json::json!({ "type": "boolean" }),
        ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 => {
            serde_json::json!({ "type": "integer" })
        }
        ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::Int64
        | ColumnType::UInt64 | ColumnType::Duration => serde_json::json!({ "type": "long" }),
        ColumnType::Float32 => serde_json::json!({ "type": "float" }),
        ColumnType::Float64 | ColumnType::Decimal { .. } => {
            serde_json::json!({ "type": "double" })
        }
        ColumnType::String | ColumnType::Bytes => serde_json::json!({ "type": "keyword" }),
        ColumnType::Json => serde_json::json!({ "type": "object", "enabled": true }),
        ColumnType::Date | ColumnType::DateTime | ColumnType::TimestampTz => {
            serde_json::json!({ "type": "date" })
        }
        ColumnType::Time => serde_json::json!({ "type": "keyword" }),
        ColumnType::Array { element_type } => es_property(element_type),
        ColumnType::Map { .. } => serde_json::json!({ "type": "object", "enabled": true }),
        ColumnType::Nullable(inner) => es_property(inner),
    }
}

fn mapping_of(columns: &[ColumnDef]) -> serde_json::Value {
    let properties: serde_json::Map<String, serde_json::Value> = columns
        .iter()
        .map(|c| (c.name.clone(), es_property(&c.column_type)))
        .collect();
    serde_json::json!({ "properties": properties })
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// Row → JSON document (positional `fN` names unless a schema provides names).
fn row_to_document(row: &Row, field_names: Option<&[String]>) -> serde_json::Value {
    let mut doc = serde_json::Map::with_capacity(row.field_count());
    for (i, field) in row.fields.iter().enumerate() {
        let name = field_names
            .and_then(|names| names.get(i))
            .cloned()
            .unwrap_or_else(|| format!("f{}", i));
        doc.insert(name, field_to_json(field));
    }
    serde_json::Value::Object(doc)
}

fn field_to_json(field: &Field) -> serde_json::Value {
    match field {
        Field::Null => serde_json::Value::Null,
        Field::Bool(b) => serde_json::Value::Bool(*b),
        Field::Int8(v) => (*v as i64).into(),
        Field::Int16(v) => (*v as i64).into(),
        Field::Int32(v) => (*v).into(),
        Field::Int64(v) => (*v).into(),
        Field::UInt8(v) => (*v as u64).into(),
        Field::UInt16(v) => (*v as u64).into(),
        Field::UInt32(v) => (*v as u64).into(),
        Field::UInt64(v) => (*v).into(),
        Field::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Field::Float64(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Field::Decimal(d) => serde_json::Value::String(d.to_string()),
        Field::String(s) => serde_json::Value::String(s.clone()),
        Field::Bytes(b) => serde_json::Value::String(hex::encode(b)),
        Field::Json(j) => j.clone(),
        Field::Date(d) => serde_json::Value::String(d.to_string()),
        Field::Time(t) => serde_json::Value::String(t.to_string()),
        Field::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Field::TimestampTz(ts) => serde_json::Value::String(ts.to_rfc3339()),
        Field::Duration(ns) => (*ns).into(),
        Field::Array(items) => {
            serde_json::Value::Array(items.iter().map(field_to_json).collect())
        }
        Field::Row(fields) => serde_json::Value::Array(
            fields.iter().map(field_to_json).collect(),
        ),
    }
}

/// Document `_id` from primary key field positions (`fN` selectors or
/// ordinals given as `#N`).
fn document_id(row: &Row, key_fields: &[String], delimiter: &str) -> Option<String> {
    if key_fields.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(key_fields.len());
    for selector in key_fields {
        let field = if let Some(ordinal) = selector.strip_prefix('#') {
            ordinal.parse::<usize>().ok().and_then(|i| row.fields.get(i))
        } else {
            selector
                .strip_prefix('f')
                .and_then(|n| n.parse::<usize>().ok())
                .and_then(|i| row.fields.get(i))
        };
        parts.push(match field {
            Some(Field::String(s)) => s.clone(),
            Some(Field::Null) | None => return None,
            Some(other) => format!("{}", other),
        });
    }
    Some(parts.join(delimiter))
}

/// Elasticsearch sink writer: NDJSON `_bulk` batches with upserts.
pub struct EsSinkWriter {
    config: EsConfig,
    client: Option<EsClient>,
    /// Field names from the provided schema (positional rows otherwise).
    field_names: Option<Vec<String>>,
    buffer: Vec<Row>,
    written: u64,
    save_mode_applied: bool,
}

impl EsSinkWriter {
    pub fn new(config: EsConfig, schema: Option<TableSchema>) -> Self {
        let field_names = schema.map(|s| s.columns.iter().map(|c| c.name.clone()).collect());
        EsSinkWriter {
            config,
            client: None,
            field_names,
            buffer: Vec::new(),
            written: 0,
            save_mode_applied: false,
        }
    }

    fn resolve_index(&self, row: Option<&Row>) -> String {
        let mut index = self.config.index.clone();
        if let Some(row) = row {
            if index.contains("${") {
                for i in 0..row.field_count() {
                    let placeholder = format!("${{f{}}}", i);
                    let value = match row.get(i) {
                        Field::String(s) => s.clone(),
                        Field::Null => String::new(),
                        other => format!("{}", other),
                    };
                    index = index.replace(&placeholder, &value);
                }
            }
        }
        index
    }

    async fn ensure_ready(&mut self, sample: Option<&Row>) -> anyhow::Result<()> {
        if self.client.is_none() {
            let client = EsClient::new(&self.config)?;
            self.client = Some(client);
        }
        if self.save_mode_applied {
            return Ok(());
        }
        let client = self.client.as_ref().expect("client");
        let index = self.resolve_index(sample);
        let exists = client.index_exists(&index).await?;
        match self.config.schema_save_mode {
            EsSchemaSaveMode::ErrorWhenNotExist if !exists => {
                anyhow::bail!("index '{}' does not exist", index);
            }
            EsSchemaSaveMode::RecreateSchema => {
                if exists {
                    client.delete_index(&index).await?;
                }
                let columns = self.columns_for(sample);
                client.create_index(&index, mapping_of(&columns)).await?;
                tracing::info!("ES sink: recreated index {}", index);
            }
            EsSchemaSaveMode::CreateWhenNotExist if !exists => {
                let columns = self.columns_for(sample);
                client.create_index(&index, mapping_of(&columns)).await?;
                tracing::info!("ES sink: auto-created index {}", index);
            }
            _ => {}
        }
        match self.config.data_save_mode {
            EsDataSaveMode::DropData if exists || self.config.schema_save_mode != EsSchemaSaveMode::Ignore => {
                client.delete_by_query(&index).await?;
            }
            EsDataSaveMode::ErrorWhenDataExists if exists => {
                let count = client.count(&index).await?;
                if count > 0 {
                    anyhow::bail!(
                        "index '{}' has {} documents and data-save-mode is error-when-data-exists",
                        index,
                        count
                    );
                }
            }
            _ => {}
        }
        self.save_mode_applied = true;
        Ok(())
    }

    /// Columns for mapping creation: schema-provided or inferred from a
    /// sample row (positional `fN` names).
    fn columns_for(&self, sample: Option<&Row>) -> Vec<ColumnDef> {
        if let Some(names) = &self.field_names {
            return names
                .iter()
                .map(|n| ColumnDef::new(n.clone(), ColumnType::String))
                .collect();
        }
        match sample {
            Some(row) => row
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| ColumnDef::new(format!("f{}", i), es_column_type_of(f)))
                .collect(),
            None => vec![ColumnDef::new("f0", ColumnType::String)],
        }
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let sample_fields = self
            .buffer
            .first()
            .map(|row| row.fields.clone());
        let sample = sample_fields.as_ref().map(|fields| {
            let mut row = Row::new(RowKind::Insert, fields.len());
            for (i, f) in fields.iter().enumerate() {
                row.set(i, f.clone());
            }
            row
        });
        self.ensure_ready(sample.as_ref()).await?;
        let rows = std::mem::take(&mut self.buffer);
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ES client unavailable"))?;

        // Group by resolved index so variable-index writes batch correctly.
        let mut bodies: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for row in &rows {
            let index = self.resolve_index(Some(row));
            let id = document_id(row, &self.config.primary_keys, &self.config.key_delimiter);
            let body = bodies.entry(index.clone()).or_default();
            match row.kind {
                RowKind::Insert | RowKind::UpdateAfter => {
                    let doc = row_to_document(row, self.field_names.as_deref());
                    match id {
                        Some(id) => {
                            body.push_str(
                                &serde_json::json!({ "update": { "_index": index, "_id": id } })
                                    .to_string(),
                            );
                            body.push('\n');
                            body.push_str(
                                &serde_json::json!({ "doc": doc, "doc_as_upsert": true })
                                    .to_string(),
                            );
                            body.push('\n');
                        }
                        None => {
                            body.push_str(
                                &serde_json::json!({ "index": { "_index": index } }).to_string(),
                            );
                            body.push('\n');
                            body.push_str(&doc.to_string());
                            body.push('\n');
                        }
                    }
                }
                RowKind::Delete | RowKind::UpdateBefore => match id {
                    Some(id) => {
                        body.push_str(
                            &serde_json::json!({ "delete": { "_index": index, "_id": id } })
                                .to_string(),
                        );
                        body.push('\n');
                    }
                    None => {
                        tracing::warn!("ES sink: delete without primary key skipped");
                    }
                },
            }
        }

        let mut attempts = 0u32;
        loop {
            let mut failed = None;
            for (index, body) in &bodies {
                if let Err(e) = client.bulk(body).await {
                    failed = Some((index.clone(), e));
                    break;
                }
            }
            match failed {
                None => break,
                Some((index, e)) if attempts < self.config.max_retry_count => {
                    attempts += 1;
                    tracing::warn!(
                        "ES bulk to {} failed (attempt {}/{}): {}",
                        index,
                        attempts,
                        self.config.max_retry_count,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(300 * u64::from(attempts))).await;
                }
                Some((_, e)) => return Err(e),
            }
        }
        self.written += rows.len() as u64;
        Ok(())
    }
}

fn es_column_type_of(field: &Field) -> ColumnType {
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
        Field::Decimal(_) => ColumnType::Decimal {
            precision: 20,
            scale: 6,
        },
        Field::String(_) => ColumnType::String,
        Field::Bytes(_) => ColumnType::Bytes,
        Field::Json(_) => ColumnType::Json,
        Field::Date(_) => ColumnType::Date,
        Field::Time(_) => ColumnType::Time,
        Field::DateTime(_) => ColumnType::DateTime,
        Field::TimestampTz(_) => ColumnType::TimestampTz,
        Field::Duration(_) => ColumnType::Duration,
        Field::Array(_) => ColumnType::Array {
            element_type: Box::new(ColumnType::String),
        },
        Field::Row(_) => ColumnType::String,
    }
}

impl SinkWriter for EsSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Err(e) = self.ensure_ready(None).await {
                tracing::warn!("ES sink: index preparation deferred to first batch: {}", e);
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
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            if self.client.is_some() {
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
            if self.client.is_some() {
                self.flush().await?;
            }
            self.client.take();
            Ok(())
        })
    }

    fn apply_schema_change(
        &mut self,
        event: &SchemaChangeEvent,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let event = event.clone();
        Box::pin(async move {
            if self.client.is_some() {
                self.flush().await?;
            }
            if let Some(client) = &self.client {
                let index = self.resolve_index(None);
                for change in &event.changes {
                    match change {
                        seatunnel_api::SchemaChange::AddColumn { .. } => {
                            // Positional documents use fN field names.
                            let effective = if self.field_names.is_none() {
                                seatunnel_api::schema::translate_positional(change)
                            } else {
                                change.clone()
                            };
                            let column = match effective {
                                seatunnel_api::SchemaChange::AddColumn { column, .. } => column,
                                _ => continue,
                            };
                            let mut props = serde_json::Map::new();
                            props.insert(
                                column.name.clone(),
                                es_property(&column.column_type),
                            );
                            client
                                .update_mapping(&index, serde_json::Value::Object(props))
                                .await?;
                            tracing::info!("ES sink: mapping updated with column '{}'", column.name);
                        }
                        // Elasticsearch cannot drop fields or change their
                        // mapping type in place.
                        seatunnel_api::SchemaChange::DropColumn { column_name, .. } => {
                            tracing::warn!(
                                "ES sink: DROP COLUMN '{}' cannot be applied to mapping (unsupported by Elasticsearch)",
                                column_name
                            );
                        }
                        seatunnel_api::SchemaChange::RenameColumn { old_name, new_name, .. } => {
                            tracing::warn!(
                                "ES sink: RENAME COLUMN '{}' -> '{}' cannot be applied to mapping (unsupported by Elasticsearch)",
                                old_name,
                                new_name
                            );
                        }
                        seatunnel_api::SchemaChange::ModifyColumn { column, .. } => {
                            tracing::warn!(
                                "ES sink: MODIFY COLUMN '{}' cannot be applied to mapping (unsupported by Elasticsearch)",
                                column.name
                            );
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

/// Elasticsearch sink connector.
pub struct EsSink {
    pub config: EsConfig,
    pub schema: Option<TableSchema>,
}

impl Sink for EsSink {
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
        Ok(Box::new(EsSinkWriter::new(self.config.clone(), self.schema.clone())))
    }

    fn restore_writer(
        &self,
        _ctx: &SinkWriterContext,
        _states: &[Vec<u8>],
    ) -> anyhow::Result<
        Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>,
    > {
        Ok(Box::new(EsSinkWriter::new(self.config.clone(), self.schema.clone())))
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

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Opaque split handle.
#[derive(Debug, Clone)]
pub struct EsSplit {
    pub id: String,
}

impl SourceSplit for EsSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Bounded scroll-based Elasticsearch source reader.
pub struct EsSourceReader {
    config: EsConfig,
    client: Option<EsClient>,
    scroll_id: Option<String>,
    hits: std::collections::VecDeque<serde_json::Value>,
    opened: bool,
    done: bool,
}

impl EsSourceReader {
    pub fn new(config: EsConfig) -> Self {
        EsSourceReader {
            config,
            client: None,
            scroll_id: None,
            hits: std::collections::VecDeque::new(),
            opened: false,
            done: false,
        }
    }
}

fn hit_to_row(hit: &serde_json::Value) -> Row {
    let id = hit.get("_id").and_then(|v| v.as_str()).unwrap_or("");
    let source = hit.get("_source").cloned().unwrap_or(serde_json::Value::Null);
    let mut fields: Vec<Field> = vec![Field::String(id.to_string())];
    match &source {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (_, v) in entries {
                fields.push(json_to_field(v));
            }
        }
        other => fields.push(json_to_field(other)),
    }
    let mut row = Row::new(RowKind::Insert, fields.len());
    for (i, f) in fields.into_iter().enumerate() {
        row.set(i, f);
    }
    row
}

fn json_to_field(v: &serde_json::Value) -> Field {
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
        other => Field::String(other.to_string()),
    }
}

impl SourceReader for EsSourceReader {
    type Output = Row;
    type Split = EsSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let client = EsClient::new(&self.config)?;
            let (scroll_id, hits) = client
                .search_open(
                    &self.config.index,
                    self.config.query.as_deref(),
                    &self.config.scroll_time,
                    self.config.scroll_size,
                )
                .await?;
            tracing::info!(
                "ES source: opened scroll on index '{}' ({} initial hits)",
                self.config.index,
                hits.len()
            );
            self.scroll_id = scroll_id;
            self.hits.extend(hits);
            self.client = Some(client);
            self.opened = true;
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if let Some(hit) = self.hits.pop_front() {
                return Ok(PollResult::Record(hit_to_row(&hit)));
            }
            if self.done || !self.opened {
                return Ok(if self.done { PollResult::EOF } else { PollResult::Empty });
            }
            let Some(client) = &self.client else {
                return Ok(PollResult::EOF);
            };
            let Some(scroll_id) = self.scroll_id.clone() else {
                self.done = true;
                return Ok(PollResult::EOF);
            };
            let hits = client.scroll_next(&scroll_id, &self.config.scroll_time).await?;
            if hits.is_empty() {
                client.scroll_close(&scroll_id).await;
                self.done = true;
                return Ok(PollResult::EOF);
            }
            self.hits.extend(hits);
            Ok(PollResult::Record(hit_to_row(&self.hits.pop_front().expect("non-empty"))))
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let done = self.done;
        Box::pin(async move {
            Ok(serde_json::to_vec(&serde_json::json!({ "done": done }))?)
        })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let (Some(client), Some(scroll_id)) = (&self.client, &self.scroll_id) {
                client.scroll_close(scroll_id).await;
            }
            self.client.take();
            Ok(())
        })
    }
}

/// Elasticsearch source connector.
pub struct EsSource {
    pub config: EsConfig,
}

impl Source for EsSource {
    type Output = Row;
    type Split = EsSplit;
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
        Ok(Box::new(EsSourceReader::new(self.config.clone())))
    }

    fn restore_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
        _state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(EsSourceReader::new(self.config.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_es_property_mapping() {
        assert_eq!(
            es_property(&ColumnType::Int64),
            serde_json::json!({ "type": "long" })
        );
        assert_eq!(
            es_property(&ColumnType::String),
            serde_json::json!({ "type": "keyword" })
        );
        assert_eq!(
            es_property(&ColumnType::Array { element_type: Box::new(ColumnType::Int32) }),
            serde_json::json!({ "type": "integer" })
        );
    }

    #[test]
    fn test_document_id_and_bulk_lines() {
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, Field::Int64(7));
        row.set(1, Field::String("a".into()));
        row.set(2, Field::String("b".into()));
        let id = document_id(&row, &["f0".to_string(), "f1".to_string()], "_").unwrap();
        assert_eq!(id, "7_a");
        assert!(document_id(&row, &[], "_").is_none());

        let doc = row_to_document(&row, None);
        assert_eq!(doc["f0"], serde_json::json!(7));
    }

    #[test]
    fn test_config_parsing() {
        let props: std::collections::HashMap<String, String> = [
            ("hosts", "es1:9200, es2:9200"),
            ("index", "users_idx"),
            ("primary-keys", "f0"),
            ("max-batch-size", "50"),
            ("schema-save-mode", "CREATE_SCHEMA_WHEN_NOT_EXIST"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let config = EsConfig::from_config(&ConnectorConfig::new(props));
        assert_eq!(config.hosts.len(), 2);
        assert_eq!(config.hosts[0], "http://es1:9200");
        assert_eq!(config.index, "users_idx");
        assert_eq!(config.primary_keys, vec!["f0".to_string()]);
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.schema_save_mode, EsSchemaSaveMode::CreateWhenNotExist);
    }

    #[test]
    fn test_hit_to_row() {
        let hit = serde_json::json!({
            "_id": "42",
            "_source": { "name": "alice", "age": 30 }
        });
        let row = hit_to_row(&hit);
        assert_eq!(row.field_count(), 3);
        assert_eq!(row.get(0), &Field::String("42".to_string()));
        assert_eq!(row.get(1), &Field::Int64(30));
        assert_eq!(row.get(2), &Field::String("alice".to_string()));
    }
}
