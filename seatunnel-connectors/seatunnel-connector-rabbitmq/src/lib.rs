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

//! RabbitMQ connector (Java: `connector-rabbitmq`).
//!
//! ## Source
//! Consumes from a queue with manual acknowledgements. The ack of a
//! delivery is deferred to `notify_checkpoint_complete` — the same
//! commit-on-checkpoint pattern the Kafka source uses for offsets — so a
//! crash before the checkpoint completes causes the broker to redeliver
//! the message (at-least-once).
//!
//! ## Sink
//! Batched publishes to an exchange/routing-key (or straight to the queue
//! through the default exchange), optionally guarded by publisher
//! confirms. At-least-once: buffered rows are flushed on batch size,
//! linger, checkpoint and close.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind,
};
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
use seatunnel_formats::MessageFormat;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// RabbitMQ configuration (source + sink).
#[derive(Debug, Clone)]
pub struct RabbitMqConfig {
    pub host: String,
    pub port: u16,
    pub virtual_host: String,
    pub username: String,
    pub password: String,
    /// Queue to consume from (source) and to declare/bind (sink).
    pub queue_name: String,
    /// Exchange to publish to (sink); empty = default exchange, which
    /// routes straight to the queue named by the routing key.
    pub exchange: String,
    /// Routing key for publishes and for the queue binding.
    pub routing_key: String,
    /// Consumer prefetch window (unacked messages per consumer).
    pub prefetch_count: u16,
    /// Mark published messages persistent (delivery_mode = 2).
    pub persistent: bool,
    /// Enable publisher confirms on the sink channel.
    pub publisher_confirm: bool,
    /// Payload format handled by seatunnel-formats.
    pub format: MessageFormat,
    /// Delimiter joining fields for TEXT payloads.
    pub field_delimiter: String,
    /// Optional column list; enables schema-based (de)serialization.
    pub columns: Vec<String>,
    pub batch_size: usize,
    /// Max time a partial sink batch may linger before it is flushed.
    pub batch_timeout_ms: u64,
    /// Poll timeout per `poll_next` call, milliseconds.
    pub poll_timeout_ms: u64,
    /// Pipeline name injected by the engine (consumer tag namespace).
    pub pipeline: String,
    /// Subtask index injected by the engine (consumer tag namespace).
    pub subtask_index: usize,
}

impl Default for RabbitMqConfig {
    fn default() -> Self {
        RabbitMqConfig {
            host: "127.0.0.1".to_string(),
            port: 5672,
            virtual_host: "/".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            queue_name: "seatunnel".to_string(),
            exchange: String::new(),
            routing_key: String::new(),
            prefetch_count: 250,
            persistent: true,
            publisher_confirm: true,
            format: MessageFormat::Json,
            field_delimiter: ",".to_string(),
            columns: Vec::new(),
            batch_size: 100,
            batch_timeout_ms: 100,
            poll_timeout_ms: 250,
            pipeline: "p0".to_string(),
            subtask_index: 0,
        }
    }
}

/// Percent-encode the unreserved set for AMQP URI components.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl RabbitMqConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        RabbitMqConfig {
            host: config.get_string("host", "127.0.0.1"),
            port: config.get_int("port", 5672).clamp(1, 65_535) as u16,
            virtual_host: config
                .get_string("virtual-host", &config.get_string("virtual_host", "/")),
            username: config.get_string("username", "guest"),
            password: config.get_string("password", "guest"),
            queue_name: config
                .get_string("queue-name", &config.get_string("queue_name", "seatunnel")),
            exchange: config.get_string("exchange", ""),
            routing_key: config.get_string("routing-key", &config.get_string("routing_key", "")),
            prefetch_count: config.get_int("prefetch-count", 250).clamp(0, 65_535) as u16,
            persistent: config.get_bool("persistent", true),
            publisher_confirm: config.get_bool("publisher-confirm", true),
            format: config
                .get("format")
                .and_then(|f| MessageFormat::from_str(f))
                .unwrap_or(MessageFormat::Json),
            field_delimiter: config.get_string("field.delimiter", ","),
            columns: config
                .get_string("columns", "")
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect(),
            batch_size: config
                .get_int("max-batch-size", config.get_int("batch.size", 100))
                .max(1) as usize,
            batch_timeout_ms: config.get_int("batch.timeout.ms", 100).max(0) as u64,
            poll_timeout_ms: config.get_int("poll.timeout.ms", 250).max(10) as u64,
            pipeline: config.get_string("pipeline.name", "p0"),
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
        }
    }

    /// Effective routing key: an explicit one, else the queue name (the
    /// default exchange routes by queue name).
    fn effective_routing_key(&self) -> String {
        if !self.routing_key.is_empty() {
            self.routing_key.clone()
        } else {
            self.queue_name.clone()
        }
    }

    /// AMQP connection URI with percent-encoded credentials and vhost.
    fn amqp_uri(&self) -> String {
        // The vhost is a single PATH SEGMENT, not a hierarchy: the
        // default vhost "/" must stay percent-encoded as %2F, and
        // stripping leading slashes would turn it into the
        // non-existent "" vhost ("vhost not found" on connection open).
        let vhost = self.virtual_host.trim();
        let vhost = if vhost.is_empty() {
            "%2F".to_string()
        } else {
            percent_encode(vhost)
        };
        format!(
            "amqp://{}:{}@{}:{}/{}",
            percent_encode(&self.username),
            percent_encode(&self.password),
            self.host,
            self.port,
            vhost
        )
    }

    /// Schema built from the configured column list (if any).
    fn schema(&self) -> Option<TableSchema> {
        if self.columns.is_empty() {
            None
        } else {
            Some(TableSchema::new(
                format!("rabbitmq.{}", self.queue_name),
                self.columns
                    .iter()
                    .map(|c| ColumnDef::new(c.clone(), ColumnType::String))
                    .collect(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Payload codec (positional, mirrors the Kafka connector)
// ---------------------------------------------------------------------------

/// Serialize a `Row` into a message payload according to the format.
fn encode_row(row: &Row, format: &MessageFormat, delimiter: &str) -> String {
    match format {
        MessageFormat::Text => row
            .fields
            .iter()
            .map(|f| match f {
                Field::String(s) => s.clone(),
                Field::Null => String::new(),
                other => format!("{other}"),
            })
            .collect::<Vec<_>>()
            .join(delimiter),
        // JSON-family formats encode as a positional array; the formats
        // crate produces named envelopes when a schema is available.
        _ => {
            let fields: Vec<serde_json::Value> =
                row.fields.iter().map(field_to_json_value).collect();
            serde_json::to_string(&fields).unwrap_or_default()
        }
    }
}

fn field_to_json_value(field: &Field) -> serde_json::Value {
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
        Field::String(v) => serde_json::Value::String(v.clone()),
        Field::Bytes(v) => serde_json::Value::String(hex::encode(v)),
        Field::Decimal(v) => serde_json::Value::String(v.to_string()),
        Field::Json(v) => v.clone(),
        Field::Date(v) => serde_json::Value::String(v.to_string()),
        Field::Time(v) => serde_json::Value::String(v.to_string()),
        Field::DateTime(v) => serde_json::Value::String(v.to_string()),
        Field::TimestampTz(v) => serde_json::Value::String(v.to_rfc3339()),
        Field::Duration(v) => (*v).into(),
        Field::Array(v) => serde_json::Value::Array(v.iter().map(field_to_json_value).collect()),
        Field::Row(v) => serde_json::Value::Array(v.iter().map(field_to_json_value).collect()),
    }
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

/// Decode a message payload into rows (schema-based via seatunnel-formats,
/// else positional JSON / single-string TEXT).
fn decode_payload(format: MessageFormat, payload: &[u8], schema: Option<&TableSchema>) -> Vec<Row> {
    if let Some(schema) = schema {
        return match seatunnel_formats::deserialize_all(format, payload, schema) {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!("rabbitmq decode failed ({}); message skipped", e);
                Vec::new()
            }
        };
    }
    match format {
        MessageFormat::Text => {
            let mut row = Row::new(RowKind::Insert, 1);
            row.set(
                0,
                Field::String(String::from_utf8_lossy(payload).to_string()),
            );
            vec![row]
        }
        _ => {
            let text = String::from_utf8_lossy(payload);
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(serde_json::Value::Object(map)) => {
                    let mut entries: Vec<_> = map.iter().collect();
                    entries.sort_by(|a, b| a.0.cmp(b.0));
                    let mut row = Row::new(RowKind::Insert, entries.len());
                    for (i, (_, v)) in entries.iter().enumerate() {
                        row.set(i, json_value_to_field(v));
                    }
                    vec![row]
                }
                Ok(serde_json::Value::Array(items)) => {
                    let mut row = Row::new(RowKind::Insert, items.len());
                    for (i, v) in items.iter().enumerate() {
                        row.set(i, json_value_to_field(v));
                    }
                    vec![row]
                }
                _ => {
                    let mut row = Row::new(RowKind::Insert, 1);
                    row.set(0, Field::String(text.to_string()));
                    vec![row]
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Opaque split handle.
#[derive(Debug, Clone)]
pub struct RabbitMqSplit {
    pub id: String,
}

impl SourceSplit for RabbitMqSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// One buffered row plus the delivery tag of its message (attached to the
/// last row of the delivery, so the ack fires exactly once all rows of
/// that message have been emitted).
type PendingRow = (Row, Option<u64>);

/// RabbitMQ source reader with deferred, checkpoint-driven acks.
pub struct RabbitMqSourceReader {
    config: RabbitMqConfig,
    schema: Option<TableSchema>,
    pending: VecDeque<PendingRow>,
    /// Delivery tags of messages whose rows were already emitted into the
    /// current checkpoint window.
    pending_acks: Vec<u64>,
    /// Tags captured at the last `snapshot_state`; acked when that
    /// checkpoint completes.
    checkpoint_acks: Vec<u64>,
    connection: Option<Connection>,
    channel: Option<Channel>,
    consumer: Option<lapin::Consumer>,
    total_emitted: u64,
}

impl RabbitMqSourceReader {
    pub fn new(config: RabbitMqConfig) -> Self {
        let schema = config.schema();
        RabbitMqSourceReader {
            config,
            schema,
            pending: VecDeque::new(),
            pending_acks: Vec::new(),
            checkpoint_acks: Vec::new(),
            connection: None,
            channel: None,
            consumer: None,
            total_emitted: 0,
        }
    }

    /// Acknowledge one delivery; failures only warn (the broker then
    /// redelivers, which is the at-least-once fallback).
    async fn ack_tag(&self, tag: u64) {
        let Some(channel) = &self.channel else { return };
        if let Err(e) = channel.basic_ack(tag, BasicAckOptions::default()).await {
            tracing::warn!("basic_ack failed (broker will redeliver): {}", e);
        }
    }
}

impl SourceReader for RabbitMqSourceReader {
    type Output = Row;
    type Split = RabbitMqSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let uri = self.config.amqp_uri();
            let connection = Connection::connect(uri.as_str(), ConnectionProperties::default())
                .await
                .map_err(|e| anyhow::anyhow!("RabbitMQ connect failed: {}", e))?;
            let channel = connection
                .create_channel()
                .await
                .map_err(|e| anyhow::anyhow!("RabbitMQ channel failed: {}", e))?;
            channel
                .basic_qos(self.config.prefetch_count, BasicQosOptions::default())
                .await
                .map_err(|e| anyhow::anyhow!("basic_qos failed: {}", e))?;
            // Declare (or verify) a durable queue; binds it to the exchange
            // when both an exchange and a routing key are configured.
            channel
                .queue_declare(
                    self.config.queue_name.as_str().into(),
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("queue_declare failed: {}", e))?;
            if !self.config.exchange.is_empty() {
                // The exchange must exist before the queue can bind to it.
                channel
                    .exchange_declare(
                        self.config.exchange.as_str().into(),
                        ExchangeKind::Direct,
                        ExchangeDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("exchange_declare failed: {}", e))?;
                let routing_key = self.config.effective_routing_key();
                channel
                    .queue_bind(
                        self.config.queue_name.as_str().into(),
                        self.config.exchange.as_str().into(),
                        routing_key.as_str().into(),
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("queue_bind failed: {}", e))?;
            }
            let tag = format!(
                "seatunnel-{}-{}",
                self.config.pipeline, self.config.subtask_index
            );
            let consumer = channel
                .basic_consume(
                    self.config.queue_name.as_str().into(),
                    tag.as_str().into(),
                    // no_ack defaults to false: manual acks only.
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("basic_consume failed: {}", e))?;
            tracing::info!(
                "RabbitMQ source: consuming queue='{}' vhost='{}' at {}:{} (prefetch={}, format={})",
                self.config.queue_name,
                self.config.virtual_host,
                self.config.host,
                self.config.port,
                self.config.prefetch_count,
                self.config.format.name()
            );
            self.connection = Some(connection);
            self.channel = Some(channel);
            self.consumer = Some(consumer);
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if let Some((row, acker)) = self.pending.pop_front() {
                if let Some(acker) = acker {
                    self.pending_acks.push(acker);
                }
                self.total_emitted += 1;
                return Ok(PollResult::Record(row));
            }
            let Some(consumer) = self.consumer.as_mut() else {
                return Ok(PollResult::Empty);
            };
            let timeout = tokio::time::timeout(
                Duration::from_millis(self.config.poll_timeout_ms),
                consumer.next(),
            )
            .await;
            match timeout {
                Ok(Some(Ok(delivery))) => {
                    let lapin::message::Delivery {
                        data, delivery_tag, ..
                    } = delivery;
                    let rows = decode_payload(self.config.format, &data, self.schema.as_ref());
                    let count = rows.len();
                    if count == 0 {
                        // Poison payload: acknowledge so the broker does not
                        // redeliver it forever.
                        self.ack_tag(delivery_tag).await;
                        return Ok(PollResult::Empty);
                    }
                    for (i, row) in rows.into_iter().enumerate() {
                        let tag = (i + 1 == count).then_some(delivery_tag);
                        self.pending.push_back((row, tag));
                    }
                    if let Some((row, tag)) = self.pending.pop_front() {
                        if let Some(tag) = tag {
                            self.pending_acks.push(tag);
                        }
                        self.total_emitted += 1;
                        return Ok(PollResult::Record(row));
                    }
                    Ok(PollResult::Empty)
                }
                Ok(Some(Err(e))) => {
                    tracing::warn!("RabbitMQ consumer error: {}", e);
                    Ok(PollResult::Empty)
                }
                Ok(None) => {
                    // Consumer cancelled server-side; surface as empty poll,
                    // the engine treats persistent empties as idle.
                    tracing::warn!("RabbitMQ consumer stream ended");
                    Ok(PollResult::Empty)
                }
                Err(_) => Ok(PollResult::Empty),
            }
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        // Capture the acks of everything emitted into this checkpoint
        // window; they are acked only when the checkpoint completes so an
        // aborted checkpoint redelivers instead of losing data.
        self.checkpoint_acks = std::mem::take(&mut self.pending_acks);
        let state = serde_json::json!({
            "total_emitted": self.total_emitted,
            "pending_acks": self.checkpoint_acks.len(),
            "buffered_rows": self.pending.len(),
        });
        Box::pin(async move { Ok(serde_json::to_vec(&state)?) })
    }

    fn notify_checkpoint_complete(
        &mut self,
        checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let tags = std::mem::take(&mut self.checkpoint_acks);
            if tags.is_empty() {
                return Ok(());
            }
            tracing::debug!(
                "RabbitMQ source: acking {} delivery/ies for checkpoint {}",
                tags.len(),
                checkpoint_id
            );
            for tag in tags {
                self.ack_tag(tag).await;
            }
            Ok(())
        })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            // Anything not yet acked is left for redelivery — the
            // at-least-once contract on restart.
            self.consumer.take();
            self.channel.take();
            if let Some(connection) = self.connection.take() {
                let _ = connection.close(0, "source closed".into()).await;
            }
            Ok(())
        })
    }
}

/// RabbitMQ source connector.
pub struct RabbitMqSource {
    pub config: RabbitMqConfig,
}

impl Source for RabbitMqSource {
    type Output = Row;
    type Split = RabbitMqSplit;
    type State = Vec<u8>;

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.config.schema()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded
    }

    fn enumerate_splits(
        &self,
        _context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        // Queue consumers scale naturally: parallel subtasks each run a
        // consumer on the same queue and the broker round-robins deliveries.
        Ok(Vec::new())
    }

    fn create_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(RabbitMqSourceReader::new(self.config.clone())))
    }

    fn restore_reader(
        &self,
        _context: seatunnel_api::source::source_reader::SourceReaderContext,
        _state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        // Position restore is implicit: unacked deliveries are redelivered
        // by the broker after the restart.
        Ok(Box::new(RabbitMqSourceReader::new(self.config.clone())))
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// RabbitMQ sink writer: buffered publishes with optional confirms.
pub struct RabbitMqSinkWriter {
    config: RabbitMqConfig,
    batch: Vec<Row>,
    total_written: usize,
    connection: Option<Connection>,
    channel: Option<Channel>,
    last_flush: Instant,
}

impl RabbitMqSinkWriter {
    pub fn new(config: RabbitMqConfig) -> Self {
        RabbitMqSinkWriter {
            config,
            batch: Vec::new(),
            total_written: 0,
            connection: None,
            channel: None,
            last_flush: Instant::now(),
        }
    }

    /// Restore counters from a serialized `snapshot_state` payload. The
    /// batch itself is never restored — unconfirmed rows are re-sent from
    /// the last checkpoint by the engine.
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: serde_json::Value = serde_json::from_slice(bytes)?;
        self.total_written = state
            .get("total_written")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        Ok(())
    }

    async fn ensure_connection(&mut self) -> anyhow::Result<()> {
        if self.channel.is_some() {
            return Ok(());
        }
        let uri = self.config.amqp_uri();
        let connection = Connection::connect(uri.as_str(), ConnectionProperties::default())
            .await
            .map_err(|e| anyhow::anyhow!("RabbitMQ connect failed: {}", e))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| anyhow::anyhow!("RabbitMQ channel failed: {}", e))?;
        if self.config.publisher_confirm {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| anyhow::anyhow!("confirm_select failed: {}", e))?;
        }
        // Publish topology: the exchange, queue and binding must EXIST
        // before the first basic.publish — publishing to a missing
        // exchange kills the channel with a 404. Idempotent declares.
        if !self.config.exchange.is_empty() {
            channel
                .exchange_declare(
                    self.config.exchange.as_str().into(),
                    ExchangeKind::Direct,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("exchange_declare failed: {}", e))?;
        }
        if !self.config.queue_name.is_empty() {
            channel
                .queue_declare(
                    self.config.queue_name.as_str().into(),
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("queue_declare failed: {}", e))?;
            if !self.config.exchange.is_empty() {
                channel
                    .queue_bind(
                        self.config.queue_name.as_str().into(),
                        self.config.exchange.as_str().into(),
                        self.config.effective_routing_key().as_str().into(),
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("queue_bind failed: {}", e))?;
            }
        }
        self.connection = Some(connection);
        self.channel = Some(channel);
        Ok(())
    }

    async fn flush_batch(&mut self) -> anyhow::Result<usize> {
        self.last_flush = Instant::now();
        self.ensure_connection().await?;
        let Some(channel) = self.channel.as_ref() else {
            anyhow::bail!("rabbitmq channel unavailable");
        };
        let records = std::mem::take(&mut self.batch);
        if records.is_empty() {
            return Ok(0);
        }
        let exchange = self.config.exchange.clone();
        let routing_key = self.config.effective_routing_key();
        let delivery_mode: u8 = if self.config.persistent { 2 } else { 1 };
        let content_type = if self.config.format == MessageFormat::Text {
            "text/plain"
        } else {
            "application/json"
        };
        let mut sent = 0usize;
        for record in &records {
            let payload = encode_row(record, &self.config.format, &self.config.field_delimiter);
            let properties = BasicProperties::default()
                .with_delivery_mode(delivery_mode)
                .with_content_type(content_type.into());
            let confirm = channel
                .basic_publish(
                    exchange.as_str().into(),
                    routing_key.as_str().into(),
                    BasicPublishOptions::default(),
                    payload.as_bytes(),
                    properties,
                )
                .await
                .map_err(|e| anyhow::anyhow!("basic_publish failed: {}", e))?;
            if self.config.publisher_confirm {
                match confirm.await {
                    Ok(Confirmation::Ack(_)) => {}
                    other => anyhow::bail!("broker did not confirm message: {:?}", other),
                }
            }
            sent += 1;
        }
        self.total_written += sent;
        Ok(sent)
    }
}

impl SinkWriter for RabbitMqSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_connection().await?;
            tracing::info!(
                "RabbitMQ sink: publisher ready for exchange='{}' routing_key='{}' at {}:{} (confirm={}, persistent={})",
                self.config.exchange,
                self.config.effective_routing_key(),
                self.config.host,
                self.config.port,
                self.config.publisher_confirm,
                self.config.persistent
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
            if self.channel.is_some() {
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
            self.channel.take();
            if let Some(connection) = self.connection.take() {
                let _ = connection.close(0, "sink closed".into()).await;
            }
            tracing::info!(
                "RabbitMQ sink: closed, total written: {}",
                self.total_written
            );
            Ok(())
        })
    }
}

/// RabbitMQ sink connector.
pub struct RabbitMqSink {
    pub config: RabbitMqConfig,
}

impl Sink for RabbitMqSink {
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
        Ok(Box::new(RabbitMqSinkWriter::new(self.config.clone())))
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
        let mut writer = RabbitMqSinkWriter::new(self.config.clone());
        if let Some(bytes) = states.last() {
            let _ = writer.restore_from_state_bytes(bytes);
        }
        Ok(Box::new(writer))
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

    fn config_from(pairs: &[(&str, &str)]) -> RabbitMqConfig {
        let props: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        RabbitMqConfig::from_config(&ConnectorConfig::new(props))
    }

    #[test]
    fn test_config_parsing() {
        let config = config_from(&[
            ("host", "rabbitmq"),
            ("port", "5673"),
            ("virtual-host", "stage"),
            ("username", "app"),
            ("password", "secret"),
            ("queue-name", "events"),
            ("routing-key", "rk"),
            ("prefetch-count", "10"),
            ("publisher-confirm", "false"),
            ("format", "text"),
        ]);
        assert_eq!(config.host, "rabbitmq");
        assert_eq!(config.port, 5673);
        assert_eq!(config.virtual_host, "stage");
        assert_eq!(config.queue_name, "events");
        assert_eq!(config.effective_routing_key(), "rk");
        assert_eq!(config.prefetch_count, 10);
        assert!(!config.publisher_confirm);
        assert_eq!(config.format, MessageFormat::Text);
        assert_eq!(config.amqp_uri(), "amqp://app:secret@rabbitmq:5673/stage");
    }

    #[test]
    fn test_amqp_uri_default_and_encoding() {
        // The default vhost "/" percent-encodes to %2F — an empty vhost
        // segment is rejected by the broker ("vhost not found").
        let config = RabbitMqConfig::default();
        assert_eq!(config.amqp_uri(), "amqp://guest:guest@127.0.0.1:5672/%2F");
        let config = config_from(&[("password", "p@ss/word")]);
        assert_eq!(
            config.amqp_uri(),
            "amqp://guest:p%40ss%2Fword@127.0.0.1:5672/%2F"
        );
    }

    #[test]
    fn test_effective_routing_key_falls_back_to_queue() {
        let config = config_from(&[("queue-name", "q1")]);
        assert_eq!(config.effective_routing_key(), "q1");
    }

    #[test]
    fn test_encode_row_text_and_json() {
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, Field::Int64(7));
        row.set(1, Field::String("a,b".into()));
        row.set(2, Field::Null);
        assert_eq!(encode_row(&row, &MessageFormat::Text, ","), "7,a,b,");
        let json = encode_row(&row, &MessageFormat::Json, ",");
        assert_eq!(json, "[7,\"a,b\",null]");
    }

    #[test]
    fn test_decode_payload_positional_json() {
        let rows = decode_payload(MessageFormat::Json, br#"{"name":"alice","age":30}"#, None);
        assert_eq!(rows.len(), 1);
        // Object entries are sorted by key: age, name.
        assert_eq!(rows[0].get(0), &Field::Int64(30));
        assert_eq!(rows[0].get(1), &Field::String("alice".into()));
    }

    #[test]
    fn test_decode_payload_text() {
        let rows = decode_payload(MessageFormat::Text, b"hello", None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), &Field::String("hello".into()));
    }
}
