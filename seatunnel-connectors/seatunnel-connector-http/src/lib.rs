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

//! HTTP connector (Java: `connector-http`).
//!
//! ## Source
//! Fetches an endpoint once (bounded) or polls it on an interval
//! (unbounded). JSON responses are mapped to positional rows: an array
//! yields one row per element, an object a single row; `data-path`
//! locates the row array inside a wrapper document.
//!
//! ## Sink
//! POSTs rows as JSON (one request per row, or batched as a JSON array /
//! NDJSON body). Success is a 2xx response; server errors and timeouts
//! are retried up to `max-retries`.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use reqwest::{Client, Method, StatusCode};
use seatunnel_api::row::{Row, RowKind};
use seatunnel_api::schema::TableSchema;
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::sink::{Sink, SinkWriterContext};
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::source::source_split_enum::SourceSplitEnumeratorContext;
use seatunnel_api::source::{Boundedness, Source};
use seatunnel_api::{ColumnDef, ColumnType, Field};
use seatunnel_connector_common::ConnectorConfig;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Body interpretation for source responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpBodyFormat {
    #[default]
    Json,
    Text,
}

/// Batch body shape for the sink when `batch-size > 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpBatchFormat {
    #[default]
    JsonArray,
    Ndjson,
}

fn parse_method(s: &str, default: Method) -> Method {
    match s.trim().to_ascii_uppercase().as_str() {
        "GET" => Method::GET,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "PATCH" => Method::PATCH,
        "DELETE" => Method::DELETE,
        "HEAD" => Method::HEAD,
        _ => default,
    }
}

/// Collect request headers. Nested YAML objects flatten to `headers.Name`
/// keys via the engine's config flattening; a plain JSON object string
/// under `headers` is accepted as a fallback.
fn parse_headers(config: &ConnectorConfig) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = config
        .to_hashmap()
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("headers.")
                .filter(|_| !key.ends_with('.'))
                .map(|name| (name.to_string(), value.clone()))
        })
        .collect();
    headers.sort();
    // Not collapsed into a let-chain: the workspace MSRV (1.85) predates
    // stabilized let-chains and CI builds against it.
    #[allow(clippy::collapsible_if)]
    if headers.is_empty() {
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(&config.get_string("headers", ""))
        {
            let mut entries: Vec<(String, serde_json::Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            headers = entries
                .into_iter()
                .map(|(k, v)| {
                    let value = match &v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k, value)
                })
                .collect();
        }
    }
    headers
}

fn json_value_to_field(v: &serde_json::Value) -> Field {
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

/// One JSON value → one positional row: objects map sorted keys to
/// positions (the ES source convention), arrays map elementwise,
/// scalars become a single-field row.
fn json_item_to_row(item: &serde_json::Value) -> Row {
    match item {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let mut row = Row::new(RowKind::Insert, entries.len());
            for (i, (_, v)) in entries.iter().enumerate() {
                row.set(i, json_value_to_field(v));
            }
            row
        }
        serde_json::Value::Array(items) => {
            let mut row = Row::new(RowKind::Insert, items.len());
            for (i, v) in items.iter().enumerate() {
                row.set(i, json_value_to_field(v));
            }
            row
        }
        scalar => {
            let mut row = Row::new(RowKind::Insert, 1);
            row.set(0, json_value_to_field(scalar));
            row
        }
    }
}

/// Row → JSON object with schema field names or positional `fN` names.
fn row_to_json_object(row: &Row, field_names: Option<&[String]>) -> serde_json::Value {
    let mut doc = serde_json::Map::with_capacity(row.field_count());
    for (i, field) in row.fields.iter().enumerate() {
        let name = field_names
            .and_then(|names| names.get(i))
            .cloned()
            .unwrap_or_else(|| format!("f{i}"));
        doc.insert(name, field_to_json(field));
    }
    serde_json::Value::Object(doc)
}

fn field_to_json(field: &Field) -> serde_json::Value {
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
        Field::Date(v) => serde_json::Value::String(v.to_string()),
        Field::Time(v) => serde_json::Value::String(v.to_string()),
        Field::DateTime(v) => serde_json::Value::String(v.to_string()),
        Field::TimestampTz(v) => serde_json::Value::String(v.to_rfc3339()),
        Field::Duration(v) => (*v).into(),
        Field::Array(v) => serde_json::Value::Array(v.iter().map(field_to_json).collect()),
        Field::Row(v) => serde_json::Value::Array(v.iter().map(field_to_json).collect()),
    }
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// HTTP source configuration.
#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    pub url: String,
    pub method: Method,
    pub headers: Vec<(String, String)>,
    /// Request body for POST/PUT/PATCH.
    pub body: Option<String>,
    /// 0 = fetch once (bounded); >0 = poll every N ms (unbounded).
    pub poll_interval_ms: u64,
    pub format: HttpBodyFormat,
    /// Dotted path locating the row array/object inside the response JSON
    /// (e.g. `data.items`).
    pub data_path: Vec<String>,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    /// Optional column names; the output schema when set.
    pub columns: Vec<String>,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        HttpSourceConfig {
            url: String::new(),
            method: Method::GET,
            headers: Vec::new(),
            body: None,
            poll_interval_ms: 0,
            format: HttpBodyFormat::Json,
            data_path: Vec::new(),
            timeout_ms: 30_000,
            max_retries: 3,
            retry_backoff_ms: 200,
            columns: Vec::new(),
        }
    }
}

impl HttpSourceConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        HttpSourceConfig {
            url: config.get_string("url", ""),
            method: parse_method(&config.get_string("method", ""), Method::GET),
            headers: parse_headers(config),
            body: {
                let body = config.get_string("body", "");
                (!body.is_empty()).then_some(body)
            },
            poll_interval_ms: config
                .get_int("poll-interval-ms", config.get_int("poll_interval_ms", 0))
                .max(0) as u64,
            format: match config.get_string("format", "json").to_lowercase().as_str() {
                "text" | "plain" => HttpBodyFormat::Text,
                _ => HttpBodyFormat::Json,
            },
            data_path: config
                .get_string("data-path", &config.get_string("data_path", ""))
                .split('.')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            timeout_ms: config
                .get_int("timeout-ms", config.get_int("timeout_ms", 30_000))
                .max(1) as u64,
            max_retries: config
                .get_int("max-retries", config.get_int("max_retry_count", 3))
                .max(0) as u32,
            retry_backoff_ms: config.get_int("retry-backoff-ms", 200).max(0) as u64,
            columns: config
                .get_string("columns", "")
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect(),
        }
    }

    fn schema(&self) -> Option<TableSchema> {
        if self.columns.is_empty() {
            None
        } else {
            Some(TableSchema::new(
                "http.source",
                self.columns
                    .iter()
                    .map(|c| ColumnDef::new(c.clone(), ColumnType::String))
                    .collect(),
            ))
        }
    }
}

/// Opaque split handle.
#[derive(Debug, Clone)]
pub struct HttpSplit {
    pub id: String,
}

impl SourceSplit for HttpSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// HTTP source reader: single fetch or interval polling.
pub struct HttpSourceReader {
    config: HttpSourceConfig,
    schema: Option<TableSchema>,
    client: Option<Client>,
    pending: VecDeque<Row>,
    opened: bool,
    done: bool,
    next_poll: Instant,
    total_emitted: u64,
}

impl HttpSourceReader {
    pub fn new(config: HttpSourceConfig) -> Self {
        let schema = config.schema();
        HttpSourceReader {
            config,
            schema,
            client: None,
            pending: VecDeque::new(),
            opened: false,
            done: false,
            next_poll: Instant::now(),
            total_emitted: 0,
        }
    }

    fn ensure_client(&mut self) -> anyhow::Result<()> {
        if self.client.is_none() {
            let client = Client::builder()
                .timeout(Duration::from_millis(self.config.timeout_ms))
                .build()
                .map_err(|e| anyhow::anyhow!("http client: {}", e))?;
            self.client = Some(client);
        }
        Ok(())
    }

    fn response_to_rows(&self, body: &str) -> anyhow::Result<Vec<Row>> {
        if let Some(schema) = &self.schema {
            // Schema configured: map JSON objects by column name.
            let mut value: serde_json::Value = serde_json::from_str(body)
                .map_err(|e| anyhow::anyhow!("invalid JSON body: {}", e))?;
            for seg in &self.config.data_path {
                value = value
                    .get(seg)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("data-path segment '{}' missing", seg))?;
            }
            let items = match value {
                serde_json::Value::Array(items) => items,
                other => vec![other],
            };
            let mut rows = Vec::with_capacity(items.len());
            for item in items {
                if !item.is_object() {
                    anyhow::bail!("schema mapping requires JSON objects, got: {}", item);
                }
                let mut map = std::collections::HashMap::new();
                for (k, v) in item.as_object().expect("object") {
                    map.insert(k.clone(), json_value_to_field(v));
                }
                rows.push(seatunnel_connector_common::row_from_map(schema, &map));
            }
            return Ok(rows);
        }
        match self.config.format {
            HttpBodyFormat::Text => {
                let mut row = Row::new(RowKind::Insert, 1);
                row.set(0, Field::String(body.to_string()));
                Ok(vec![row])
            }
            HttpBodyFormat::Json => {
                let mut value: serde_json::Value = serde_json::from_str(body)
                    .map_err(|e| anyhow::anyhow!("invalid JSON body: {}", e))?;
                for seg in &self.config.data_path {
                    value = value
                        .get(seg)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("data-path segment '{}' missing", seg))?;
                }
                match value {
                    serde_json::Value::Array(items) => {
                        Ok(items.iter().map(json_item_to_row).collect())
                    }
                    other => Ok(vec![json_item_to_row(&other)]),
                }
            }
        }
    }

    async fn fetch(&mut self) -> anyhow::Result<Vec<Row>> {
        let Some(client) = self.client.clone() else {
            return Ok(Vec::new());
        };
        let url = self.config.url.clone();
        if url.is_empty() {
            anyhow::bail!("http source requires a 'url' option");
        }
        let mut attempts = 0u32;
        loop {
            let mut request = client.request(self.config.method.clone(), &url);
            let mut has_content_type = false;
            for (name, value) in &self.config.headers {
                if name.eq_ignore_ascii_case("content-type") {
                    has_content_type = true;
                }
                request = request.header(name, value);
            }
            let mut request = request;
            if let Some(body) = &self.config.body {
                if !has_content_type {
                    request = request.header("Content-Type", "application/json");
                }
                request = request.body(body.clone());
            }
            match request.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await?;
                    if status.is_success() {
                        return self.response_to_rows(&text);
                    }
                    let retryable =
                        status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS;
                    if !retryable || attempts >= self.config.max_retries {
                        anyhow::bail!("http request failed ({}): {}", status, text);
                    }
                }
                Err(e) => {
                    if attempts >= self.config.max_retries {
                        anyhow::bail!("http request error: {}", e);
                    }
                }
            }
            attempts += 1;
            tokio::time::sleep(Duration::from_millis(
                self.config.retry_backoff_ms * u64::from(attempts),
            ))
            .await;
        }
    }
}

impl SourceReader for HttpSourceReader {
    type Output = Row;
    type Split = HttpSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_client()?;
            let rows = self.fetch().await?;
            tracing::info!(
                "HTTP source: fetched {} row(s) from {} {}",
                rows.len(),
                self.config.method,
                self.config.url
            );
            self.pending.extend(rows);
            self.opened = true;
            self.next_poll = Instant::now() + Duration::from_millis(self.config.poll_interval_ms);
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
            if self.config.poll_interval_ms == 0 {
                // Single-shot fetch: everything drained, end of stream.
                self.done = true;
                return Ok(PollResult::EOF);
            }
            let now = Instant::now();
            if now < self.next_poll {
                // Wait out the remaining interval in short slices so
                // checkpoint barriers keep flowing between polls.
                let remaining = self.next_poll - now;
                tokio::time::sleep(remaining.min(Duration::from_millis(200))).await;
                return Ok(PollResult::Empty);
            }
            let rows = self.fetch().await?;
            self.next_poll = now + Duration::from_millis(self.config.poll_interval_ms);
            let mut iter = rows.into_iter();
            match iter.next() {
                None => Ok(PollResult::Empty),
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
            "total_emitted": self.total_emitted,
            "done": self.done,
        });
        Box::pin(async move { Ok(serde_json::to_vec(&state)?) })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.client.take();
            Ok(())
        })
    }
}

/// HTTP source connector.
pub struct HttpSource {
    pub config: HttpSourceConfig,
}

impl Source for HttpSource {
    type Output = Row;
    type Split = HttpSplit;
    type State = Vec<u8>;

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.config.schema()
    }

    fn boundedness(&self) -> Boundedness {
        if self.config.poll_interval_ms > 0 {
            Boundedness::Unbounded
        } else {
            Boundedness::Bounded
        }
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
        Ok(Box::new(HttpSourceReader::new(self.config.clone())))
    }

    fn restore_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
        _state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        // Polling sources have no server-side cursor: a restart re-fetches
        // the current document (documented at-least-once duplication).
        Ok(Box::new(HttpSourceReader::new(self.config.clone())))
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// HTTP sink configuration.
#[derive(Debug, Clone)]
pub struct HttpSinkConfig {
    pub url: String,
    pub method: Method,
    pub headers: Vec<(String, String)>,
    /// 1 = one request per row (default); >1 = batched bodies.
    pub batch_size: usize,
    pub batch_format: HttpBatchFormat,
    pub batch_timeout_ms: u64,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl Default for HttpSinkConfig {
    fn default() -> Self {
        HttpSinkConfig {
            url: String::new(),
            method: Method::POST,
            headers: Vec::new(),
            batch_size: 1,
            batch_format: HttpBatchFormat::JsonArray,
            batch_timeout_ms: 100,
            timeout_ms: 30_000,
            max_retries: 3,
            retry_backoff_ms: 200,
        }
    }
}

impl HttpSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        HttpSinkConfig {
            url: config.get_string("url", ""),
            method: parse_method(&config.get_string("method", ""), Method::POST),
            headers: parse_headers(config),
            batch_size: config
                .get_int("max-batch-size", config.get_int("batch.size", 1))
                .max(1) as usize,
            batch_format: match config
                .get_string("batch-format", "json-array")
                .to_lowercase()
                .replace(['-', '_'], "")
                .as_str()
            {
                "ndjson" | "newlinejson" => HttpBatchFormat::Ndjson,
                _ => HttpBatchFormat::JsonArray,
            },
            batch_timeout_ms: config.get_int("batch.timeout.ms", 100).max(0) as u64,
            timeout_ms: config
                .get_int("timeout-ms", config.get_int("timeout_ms", 30_000))
                .max(1) as u64,
            max_retries: config
                .get_int("max-retries", config.get_int("max_retry_count", 3))
                .max(0) as u32,
            retry_backoff_ms: config.get_int("retry-backoff-ms", 200).max(0) as u64,
        }
    }
}

/// HTTP sink writer: per-row or batched JSON requests with retries.
pub struct HttpSinkWriter {
    config: HttpSinkConfig,
    field_names: Option<Vec<String>>,
    client: Option<Client>,
    batch: Vec<Row>,
    total_written: usize,
    last_flush: Instant,
}

impl HttpSinkWriter {
    pub fn new(config: HttpSinkConfig, schema: Option<TableSchema>) -> Self {
        let field_names = schema.map(|s| s.columns.iter().map(|c| c.name.clone()).collect());
        HttpSinkWriter {
            config,
            field_names,
            client: None,
            batch: Vec::new(),
            total_written: 0,
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

    fn ensure_client(&mut self) -> anyhow::Result<()> {
        if self.client.is_none() {
            let client = Client::builder()
                .timeout(Duration::from_millis(self.config.timeout_ms))
                .build()
                .map_err(|e| anyhow::anyhow!("http client: {}", e))?;
            self.client = Some(client);
        }
        Ok(())
    }

    /// Serialize the buffered rows into one request body per flush.
    fn build_requests(&self, rows: &[Row]) -> Vec<String> {
        if self.config.batch_size <= 1 {
            return rows
                .iter()
                .map(|row| row_to_json_object(row, self.field_names.as_deref()).to_string())
                .collect();
        }
        let documents: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| row_to_json_object(row, self.field_names.as_deref()))
            .collect();
        match self.config.batch_format {
            HttpBatchFormat::JsonArray => documents
                .chunks(self.config.batch_size)
                .map(|chunk| serde_json::Value::Array(chunk.to_vec()).to_string())
                .collect(),
            HttpBatchFormat::Ndjson => documents
                .chunks(self.config.batch_size)
                .map(|chunk| {
                    chunk
                        .iter()
                        .map(|doc| doc.to_string())
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .collect(),
        }
    }

    async fn send_request(&self, body: String) -> anyhow::Result<()> {
        let Some(client) = &self.client else {
            anyhow::bail!("http client unavailable");
        };
        let mut attempts = 0u32;
        loop {
            let mut request = client.request(self.config.method.clone(), &self.config.url);
            let mut has_content_type = false;
            for (name, value) in &self.config.headers {
                if name.eq_ignore_ascii_case("content-type") {
                    has_content_type = true;
                }
                request = request.header(name, value);
            }
            let mut request = request;
            if !has_content_type {
                request = request.header("Content-Type", "application/json");
            }
            let request = request.body(body.clone());
            match request.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(());
                    }
                    let text = resp.text().await.unwrap_or_default();
                    let retryable =
                        status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS;
                    if !retryable || attempts >= self.config.max_retries {
                        anyhow::bail!("http sink request failed ({}): {}", status, text);
                    }
                }
                Err(e) => {
                    if attempts >= self.config.max_retries {
                        anyhow::bail!("http sink request error: {}", e);
                    }
                }
            }
            attempts += 1;
            tokio::time::sleep(Duration::from_millis(
                self.config.retry_backoff_ms * u64::from(attempts),
            ))
            .await;
        }
    }

    async fn flush_batch(&mut self) -> anyhow::Result<usize> {
        self.last_flush = Instant::now();
        self.ensure_client()?;
        let rows = std::mem::take(&mut self.batch);
        if rows.is_empty() {
            return Ok(0);
        }
        if self.config.url.is_empty() {
            anyhow::bail!("http sink requires a 'url' option");
        }
        for body in self.build_requests(&rows) {
            self.send_request(body).await?;
        }
        self.total_written += rows.len();
        Ok(rows.len())
    }
}

impl SinkWriter for HttpSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_client()?;
            tracing::info!(
                "HTTP sink: ready for {} {} (batch-size={}, format={:?})",
                self.config.method,
                self.config.url,
                self.config.batch_size,
                self.config.batch_format
            );
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        self.batch.push(record);
        let full = self.batch.len() >= self.config.batch_size;
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
            if self.client.is_some() {
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
            self.client.take();
            tracing::info!("HTTP sink: closed, total written: {}", self.total_written);
            Ok(())
        })
    }
}

/// HTTP sink connector.
pub struct HttpSink {
    pub config: HttpSinkConfig,
    pub schema: Option<TableSchema>,
}

impl Sink for HttpSink {
    type Input = Row;
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
        Ok(Box::new(HttpSinkWriter::new(
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
        let mut writer = HttpSinkWriter::new(self.config.clone(), self.schema.clone());
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

    fn config_from(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let props: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ConnectorConfig::new(props)
    }

    #[test]
    fn test_parse_headers_dotted_and_json() {
        let config = config_from(&[
            ("headers.Authorization", "Bearer t"),
            ("headers.X-Api-Key", "k"),
        ]);
        assert_eq!(
            parse_headers(&config),
            vec![
                ("Authorization".to_string(), "Bearer t".to_string()),
                ("X-Api-Key".to_string(), "k".to_string()),
            ]
        );
        let config = config_from(&[("headers", r#"{"b":"2","a":"1"}"#)]);
        assert_eq!(
            parse_headers(&config),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn test_source_config_defaults() {
        let config = HttpSourceConfig::from_config(&config_from(&[("url", "http://x/api")]));
        assert_eq!(config.url, "http://x/api");
        assert_eq!(config.method, Method::GET);
        assert_eq!(config.poll_interval_ms, 0);
        assert_eq!(config.format, HttpBodyFormat::Json);
        assert!(config.data_path.is_empty());
    }

    #[test]
    fn test_source_config_overrides() {
        let config = HttpSourceConfig::from_config(&config_from(&[
            ("url", "http://x/api"),
            ("method", "post"),
            ("body", "{}"),
            ("poll-interval-ms", "5000"),
            ("data-path", "data.items"),
            ("format", "text"),
        ]));
        assert_eq!(config.method, Method::POST);
        assert_eq!(config.body.as_deref(), Some("{}"));
        assert_eq!(config.poll_interval_ms, 5000);
        assert_eq!(
            config.data_path,
            vec!["data".to_string(), "items".to_string()]
        );
        assert_eq!(config.format, HttpBodyFormat::Text);
    }

    #[test]
    fn test_json_item_to_row_object_sorted() {
        let item = serde_json::json!({"name": "alice", "age": 30});
        let row = json_item_to_row(&item);
        assert_eq!(row.field_count(), 2);
        assert_eq!(row.get(0), &Field::Int64(30));
        assert_eq!(row.get(1), &Field::String("alice".into()));
    }

    #[test]
    fn test_sink_request_bodies() {
        let config = HttpSinkConfig::default();
        let writer = HttpSinkWriter::new(config, None);
        let mut row = Row::new(RowKind::Insert, 2);
        row.set(0, Field::Int64(1));
        row.set(1, Field::String("a".into()));
        let mut row2 = Row::new(RowKind::Insert, 2);
        row2.set(0, Field::Int64(2));
        row2.set(1, Field::String("b".into()));

        // Per-row mode.
        let bodies = writer.build_requests(&[row.clone()]);
        assert_eq!(bodies, vec![r#"{"f0":1,"f1":"a"}"#]);

        // Batched JSON array.
        let config = HttpSinkConfig {
            batch_size: 10,
            ..Default::default()
        };
        let writer = HttpSinkWriter::new(config, None);
        let bodies = writer.build_requests(&[row, row2.clone()]);
        assert_eq!(bodies, vec![r#"[{"f0":1,"f1":"a"},{"f0":2,"f1":"b"}]"#]);

        // Batched NDJSON.
        let config = HttpSinkConfig {
            batch_size: 10,
            batch_format: HttpBatchFormat::Ndjson,
            ..Default::default()
        };
        let writer = HttpSinkWriter::new(config, None);
        let bodies = writer.build_requests(&[row2]);
        assert_eq!(bodies, vec![r#"{"f0":2,"f1":"b"}"#]);
    }

    #[test]
    fn test_row_to_json_object_with_names() {
        let schema = TableSchema::new(
            "t",
            vec![
                ColumnDef::new("id".to_string(), ColumnType::Int64),
                ColumnDef::new("name".to_string(), ColumnType::String),
            ],
        );
        let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let mut row = Row::new(RowKind::Insert, 2);
        row.set(0, Field::Int64(9));
        row.set(1, Field::String("n".into()));
        assert_eq!(
            row_to_json_object(&row, Some(&names)),
            serde_json::json!({"id": 9, "name": "n"})
        );
    }
}
