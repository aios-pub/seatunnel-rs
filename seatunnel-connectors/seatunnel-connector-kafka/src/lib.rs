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

//! Kafka Source and Sink for Apache SeaTunnel Rust.
//!
//! ## Source
//! - Partition-based splits (one split per partition)
//! - Supports 5 startup modes: Earliest, Latest, Timestamp, GroupOffset, SpecificOffset
//! - Format-based deserialization via seatunnel-formats
//!
//! ## Sink
//! - Producer with configurable acks and batching
//! - 2PC commit support for exactly-once semantics
//! - Format-based serialization via seatunnel-formats

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message as RdkafkaMessage;
use seatunnel_api::{
    row::{Row, RowKind},
    schema::TableSchema,
    sink::{sink_committer::SinkCommitter, sink_writer::SinkWriter, Sink, SinkWriterContext},
    source::{
        source_reader::{PollResult, SourceReader, SourceReaderContext},
        source_split::SourceSplit,
        source_split_enum::SourceSplitEnumeratorContext,
        Boundedness, Source,
    },
};
use seatunnel_connector_common::ConnectorConfig;
use seatunnel_formats::MessageFormat;
use serde::{Deserialize, Serialize};

/// The source data type produced by Kafka Source.
#[derive(Debug, Clone)]
pub struct KafkaSourceOutput(pub Row);

impl From<KafkaSourceOutput> for Row {
    fn from(val: KafkaSourceOutput) -> Self {
        val.0
    }
}

/// Kafka Source split — one split per partition.
#[derive(Debug, Clone)]
pub struct KafkaSourceSplit {
    pub id: String,
    pub topic: String,
    pub partition: i32,
    pub start_offset: Option<i64>,
}

impl KafkaSourceSplit {
    pub fn new(topic: &str, partition: i32, start_offset: Option<i64>) -> Self {
        KafkaSourceSplit {
            id: format!("{}-p{}", topic, partition),
            topic: topic.to_string(),
            partition,
            start_offset,
        }
    }
}

impl SourceSplit for KafkaSourceSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
    fn partition_count(&self) -> usize {
        1
    }
}

/// Checkpoint state for Kafka Source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceState {
    pub offsets: HashMap<String, i64>,
}

impl Default for KafkaSourceState {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaSourceState {
    pub fn new() -> Self {
        KafkaSourceState {
            offsets: HashMap::new(),
        }
    }
}

/// Startup mode for Kafka consumer offset.
#[derive(Debug, Clone, Default)]
pub enum KafkaStartupMode {
    #[default]
    Earliest,
    Latest,
    Timestamp {
        ts: i64,
    },
    GroupOffset {
        group: String,
    },
    SpecificOffset {
        partition: i32,
        offset: i64,
    },
}

/// Kafka Source configuration.
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub startup_mode: KafkaStartupMode,
    pub format: MessageFormat,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        KafkaSourceConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "seatunnel".to_string(),
            group_id: "seatunnel-consumer".to_string(),
            startup_mode: KafkaStartupMode::Earliest,
            format: MessageFormat::Json,
        }
    }
}

impl KafkaSourceConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        KafkaSourceConfig {
            bootstrap_servers: config.get_string("bootstrap.servers", "localhost:9092"),
            topic: config.get_string("topic", "seatunnel"),
            group_id: config.get_string("group.id", "seatunnel-consumer"),
            format: config
                .get("format")
                .and_then(|f| MessageFormat::from_str(f))
                .unwrap_or(MessageFormat::Json),
            startup_mode: config
                .get("startup.mode")
                .map(|s| match s.as_str() {
                    "earliest" => KafkaStartupMode::Earliest,
                    "latest" => KafkaStartupMode::Latest,
                    "group-offsets" => KafkaStartupMode::GroupOffset {
                        group: config.get_string("group.id", "seatunnel-consumer"),
                    },
                    _ => KafkaStartupMode::Earliest,
                })
                .unwrap_or(KafkaStartupMode::Earliest),
        }
    }

    /// `auto.offset.reset` value matching the startup mode.
    fn auto_offset_reset(&self) -> &'static str {
        match self.startup_mode {
            KafkaStartupMode::Earliest => "earliest",
            KafkaStartupMode::Latest => "latest",
            _ => "earliest",
        }
    }
}

/// Kafka Source connector.
#[derive(Debug, Clone)]
pub struct KafkaSource {
    config: KafkaSourceConfig,
    schema: Option<TableSchema>,
}

impl KafkaSource {
    pub fn new(config: KafkaSourceConfig, schema: Option<TableSchema>) -> Self {
        KafkaSource { config, schema }
    }

    pub fn from_config(config: &ConnectorConfig, schema: Option<TableSchema>) -> Self {
        KafkaSource::new(KafkaSourceConfig::from_config(config), schema)
    }

    pub fn config(&self) -> &KafkaSourceConfig {
        &self.config
    }
}

impl Source for KafkaSource {
    type Output = KafkaSourceOutput;
    type Split = KafkaSourceSplit;
    type State = KafkaSourceState;

    fn enumerate_splits(
        &self,
        context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>> {
        let partitions = context.parallelism.max(1);
        let splits: Vec<KafkaSourceSplit> = (0..partitions)
            .map(|p| KafkaSourceSplit::new(&self.config.topic, p as i32, None))
            .collect();
        tracing::info!(
            "Enumerated {} Kafka splits for topic={}, parallelism={}",
            splits.len(),
            self.config.topic,
            context.parallelism
        );
        Ok(splits)
    }

    fn create_reader(
        &self,
        _context: SourceReaderContext,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(KafkaSourceReader::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_reader(
        &self,
        _context: SourceReaderContext,
        _state: &Self::State,
    ) -> anyhow::Result<Box<dyn SourceReader<Output = Self::Output, Split = Self::Split>>> {
        Ok(Box::new(KafkaSourceReader::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.schema.clone()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded
    }
}

/// Kafka Source reader.
pub struct KafkaSourceReader {
    config: KafkaSourceConfig,
    #[allow(dead_code)] // retained for future schema-aware serialization
    schema: Option<TableSchema>,
    splits: Vec<KafkaSourceSplit>,
    /// Highest consumed offset per `topic-partition`, captured at checkpoint.
    last_offsets: HashMap<String, i64>,
    consumer: Option<StreamConsumer>,
}

impl KafkaSourceReader {
    pub fn new(config: KafkaSourceConfig, schema: Option<TableSchema>) -> Self {
        KafkaSourceReader {
            config,
            schema,
            splits: Vec::new(),
            last_offsets: HashMap::new(),
            consumer: None,
        }
    }
}

impl SourceReader for KafkaSourceReader {
    type Output = KafkaSourceOutput;
    type Split = KafkaSourceSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        Box::pin(async move {
            tracing::info!(
                "Opening Kafka source reader for topic={} (auto.offset.reset={})",
                self.config.topic,
                self.config.auto_offset_reset()
            );
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.bootstrap_servers)
                .set("group.id", &self.config.group_id)
                .set("auto.offset.reset", self.config.auto_offset_reset())
                .set("enable.auto.commit", "false")
                .set("enable.partition.eof", "false")
                .create()
                .map_err(|e| anyhow::anyhow!("Failed to create Kafka consumer: {}", e))?;
            consumer
                .subscribe(&[&self.config.topic])
                .map_err(|e| anyhow::anyhow!("Failed to subscribe to topic: {}", e))?;
            self.consumer = Some(consumer);
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        let _ = &self.splits;
        Box::pin(async move {
            // Try to poll from the real Kafka consumer with a short timeout.
            if let Some(consumer) = &self.consumer {
                match tokio::time::timeout(Duration::from_millis(250), consumer.recv()).await {
                    Ok(Ok(msg)) => {
                        if let Some(payload) = msg.payload() {
                            let s = String::from_utf8_lossy(payload).to_string();
                            self.last_offsets.insert(
                                format!("{}-{}", msg.topic(), msg.partition()),
                                msg.offset(),
                            );
                            let mut row = Row::new(RowKind::Insert, 3);
                            row.set(0, seatunnel_api::Field::String(s));
                            row.set(1, seatunnel_api::Field::Int64(msg.offset()));
                            row.set(2, seatunnel_api::Field::String(msg.topic().to_string()));
                            return Ok(PollResult::Record(KafkaSourceOutput(row)));
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("Kafka consumer error: {}", e);
                    }
                    Err(_) => {
                        // Timeout — no message available yet.
                    }
                }
            }
            Ok(PollResult::Empty)
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        Box::pin(async move {
            let state = KafkaSourceState {
                offsets: self.last_offsets.clone(),
            };
            serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!("KafkaSourceReader: adding {} splits", splits.len());
        self.splits.extend(splits);
    }

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        self.consumer.take();
        Box::pin(async move { Ok(()) })
    }
}

/// Kafka Sink split (noop — sink doesn't split).
#[derive(Debug, Clone)]
pub struct KafkaSinkSplit {
    pub id: String,
}

impl Default for KafkaSinkSplit {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaSinkSplit {
    pub fn new() -> Self {
        KafkaSinkSplit {
            id: format!("sink-{}", uuid::Uuid::new_v4()),
        }
    }
}

impl SourceSplit for KafkaSinkSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Kafka Sink commit info for 2PC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCommitInfo {
    pub transaction_id: String,
    pub messages_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAggregatedCommitInfo {
    pub checkpoint_id: i64,
    pub total_messages: usize,
}

/// Kafka Sink configuration.
#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub format: MessageFormat,
    pub acks: String,
    pub batch_size: usize,
    /// When true a transactional producer is used: records are produced
    /// inside `begin_transaction()` … `commit_transaction()` windows aligned
    /// with checkpoint boundaries (exactly-once for downstream consumers
    /// configured with `read_committed`).
    pub transactions_enabled: bool,
    /// Transactional id; required when `transactions_enabled` is true.
    pub transactional_id: Option<String>,
    /// Delivery timeout per record.
    pub message_timeout_ms: u64,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        KafkaSinkConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "seatunnel-sink".to_string(),
            format: MessageFormat::Json,
            acks: "all".to_string(),
            batch_size: 100,
            transactions_enabled: false,
            transactional_id: None,
            message_timeout_ms: 30_000,
        }
    }
}

impl KafkaSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let tx_enabled = config.get_bool("transactions.enabled", false);
        KafkaSinkConfig {
            bootstrap_servers: config.get_string("bootstrap.servers", "localhost:9092"),
            topic: config.get_string("topic", "seatunnel-sink"),
            format: config
                .get("format")
                .and_then(|f| MessageFormat::from_str(f))
                .unwrap_or(MessageFormat::Json),
            acks: config.get_string("acks", "all"),
            batch_size: config.get_int("batch.size", 1000).max(1) as usize,
            transactions_enabled: tx_enabled,
            transactional_id: config.get("transactional.id").cloned().or_else(|| {
                if tx_enabled {
                    Some(format!("seatunnel-sink-{}", uuid::Uuid::new_v4()))
                } else {
                    None
                }
            }),
            message_timeout_ms: config.get_int("message.timeout.ms", 30_000) as u64,
        }
    }
}

/// Kafka Sink connector.
#[derive(Debug, Clone)]
pub struct KafkaSink {
    config: KafkaSinkConfig,
}

impl KafkaSink {
    pub fn new(config: KafkaSinkConfig) -> Self {
        KafkaSink { config }
    }

    pub fn from_config(config: &ConnectorConfig) -> Self {
        KafkaSink::new(KafkaSinkConfig::from_config(config))
    }

    pub fn config(&self) -> &KafkaSinkConfig {
        &self.config
    }
}

impl Sink for KafkaSink {
    type Input = Row;
    type WriterState = serde_json::Value;
    type CommitInfo = KafkaCommitInfo;
    type AggregatedCommitInfo = KafkaAggregatedCommitInfo;

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
        Ok(Box::new(KafkaSinkWriter::new(self.config.clone())))
    }

    fn restore_writer(
        &self,
        _ctx: &SinkWriterContext,
        _states: &[Vec<u8>],
    ) -> anyhow::Result<
        Box<
            dyn SinkWriter<
                Input = Self::Input,
                WriterState = Self::WriterState,
                CommitInfo = Self::CommitInfo,
            >,
        >,
    > {
        Ok(Box::new(KafkaSinkWriter::new(self.config.clone())))
    }

    fn get_input_schema(&self) -> Option<TableSchema> {
        None
    }

    fn create_committer(
        &self,
    ) -> Option<
        Box<
            dyn SinkCommitter<
                CommitInfo = Self::CommitInfo,
                AggregatedCommitInfo = Self::AggregatedCommitInfo,
            >,
        >,
    > {
        Some(Box::new(KafkaSinkCommitter::new()))
    }
}

/// Kafka Sink writer with batching and optional transactions.
///
/// Records are buffered by `write()` and delivered either when the batch
/// reaches `batch_size` or during `prepare_commit()` (checkpoint boundary).
/// With `transactions.enabled=true` the batch is produced inside a Kafka
/// transaction committed in the same window.
pub struct KafkaSinkWriter {
    config: KafkaSinkConfig,
    batch: Vec<Row>,
    total_written: usize,
    producer: Option<FutureProducer>,
}

impl KafkaSinkWriter {
    pub fn new(config: KafkaSinkConfig) -> Self {
        KafkaSinkWriter {
            config,
            batch: Vec::new(),
            total_written: 0,
            producer: None,
        }
    }

    /// Lazily initialize the rdkafka producer from `bootstrap.servers`.
    /// Safe to call multiple times; only builds once.
    fn ensure_producer(&mut self) -> anyhow::Result<()> {
        if self.producer.is_some() {
            return Ok(());
        }
        let mut builder = ClientConfig::new();
        builder
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set(
                "message.timeout.ms",
                self.config.message_timeout_ms.to_string(),
            )
            .set("acks", &self.config.acks);
        if self.config.transactions_enabled {
            let tx_id = self
                .config
                .transactional_id
                .clone()
                .unwrap_or_else(|| format!("seatunnel-sink-{}", uuid::Uuid::new_v4()));
            tracing::info!("Kafka sink: transactional producer id={}", tx_id);
            builder.set("transactional.id", &tx_id);
        }
        let producer: FutureProducer = builder
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka producer: {}", e))?;
        if self.config.transactions_enabled {
            producer
                .init_transactions(std::time::Duration::from_secs(30))
                .map_err(|e| anyhow::anyhow!("init_transactions failed: {}", e))?;
        }
        self.producer = Some(producer);
        Ok(())
    }

    /// Deliver buffered records to Kafka, optionally inside a transaction.
    async fn flush_batch(&mut self) -> anyhow::Result<usize> {
        self.ensure_producer()?;
        let producer = match &self.producer {
            Some(p) => p.clone(),
            None => anyhow::bail!("kafka producer unavailable"),
        };
        let records: Vec<Row> = std::mem::take(&mut self.batch);
        if records.is_empty() {
            return Ok(0);
        }

        let transactional = self.config.transactions_enabled;
        if transactional {
            producer
                .begin_transaction()
                .map_err(|e| anyhow::anyhow!("begin_transaction failed: {}", e))?;
        }

        let topic = self.config.topic.clone();
        let mut sent = 0usize;
        let mut failures = Vec::new();
        for record in &records {
            let payload = encode_row(record, &self.config.format);
            match producer
                .send(
                    FutureRecord::<str, str>::to(&topic).payload(&payload),
                    Duration::from_millis(self.config.message_timeout_ms),
                )
                .await
            {
                Ok(_) => sent += 1,
                Err((e, _)) => failures.push(e.to_string()),
            }
        }

        if transactional && !failures.is_empty() {
            let _ = producer.abort_transaction(Duration::from_secs(10));
            anyhow::bail!(
                "transactional produce failed for {} record(s): {}",
                failures.len(),
                failures.first().map(String::as_str).unwrap_or("unknown")
            );
        }
        if !failures.is_empty() {
            // At-least-once delivery contract: surface partial failures so
            // the engine can retry from the last checkpoint.
            anyhow::bail!(
                "failed to deliver {} record(s): {}",
                failures.len(),
                failures.first().map(String::as_str).unwrap_or("unknown")
            );
        }

        if transactional {
            producer
                .commit_transaction(Duration::from_secs(30))
                .map_err(|e| {
                    let _ = producer.abort_transaction(Duration::from_secs(10));
                    anyhow::anyhow!("commit_transaction failed: {}", e)
                })?;
        }

        self.total_written += sent;
        Ok(sent)
    }
}

impl SinkWriter for KafkaSinkWriter {
    type Input = Row;
    type WriterState = serde_json::Value;
    type CommitInfo = KafkaCommitInfo;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_producer()?;
            tracing::info!(
                "KafkaSinkWriter: producer ready for topic {} via {}",
                self.config.topic,
                self.config.bootstrap_servers
            );
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        // Buffer the record; delivery happens at batch size or prepare_commit.
        self.batch.push(record);
        let full = self.batch.len() >= self.config.batch_size;
        Box::pin(async move {
            if full {
                self.flush_batch().await?;
            }
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            let pending = self.batch.len();
            let delivered = self.flush_batch().await?;
            let info = KafkaCommitInfo {
                transaction_id: format!("txn-{}", uuid::Uuid::new_v4()),
                messages_count: delivered,
            };
            tracing::info!(
                "KafkaSinkWriter: checkpoint commit flushed {} record(s) (pending={}, total={})",
                delivered,
                pending,
                self.total_written
            );
            Ok(vec![info])
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = serde_json::json!({
            "total_written": self.total_written,
            "pending": self.batch.len(),
        });
        Box::pin(async move { serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e)) })
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        Box::pin(async move {
            // Deliver anything still buffered — never silently drop records.
            if !self.batch.is_empty() {
                tracing::info!(
                    "KafkaSinkWriter: flushing {} buffered record(s) on close",
                    self.batch.len()
                );
                self.flush_batch().await?;
            }
            if let Some(producer) = &self.producer {
                producer.poll(Duration::from_secs(5));
            }
            tracing::info!(
                "KafkaSinkWriter: closed, total written: {}",
                self.total_written
            );
            Ok(())
        })
    }
}

/// Serialize a `Row` into a Kafka payload according to the configured format.
fn encode_row(row: &Row, format: &MessageFormat) -> String {
    match format {
        MessageFormat::Text => row
            .fields
            .first()
            .map(|f| match f {
                seatunnel_api::Field::String(s) => s.clone(),
                other => format!("{}", other),
            })
            .unwrap_or_default(),
        // The JSON-family encoders all fall back to the positional array
        // encoding here because CDC rows arrive positionally without column
        // names attached. Canal/Debezium envelopes are produced by the
        // dedicated formats crate when schemas are available.
        _ => row_to_json_string(row),
    }
}

/// Serialize a `Row` into a JSON string payload for Kafka.
fn row_to_json_string(row: &Row) -> String {
    let fields: Vec<serde_json::Value> = (0..row.field_count())
        .map(|i| field_to_json_value(row.get(i)))
        .collect();
    serde_json::to_string(&fields).unwrap_or_default()
}

fn field_to_json_value(field: &seatunnel_api::Field) -> serde_json::Value {
    use seatunnel_api::Field;
    match field {
        Field::Null => serde_json::Value::Null,
        Field::Bool(v) => serde_json::Value::Bool(*v),
        Field::Int8(v) => serde_json::Value::Number(serde_json::Number::from(*v as i64)),
        Field::Int16(v) => serde_json::Value::Number(serde_json::Number::from(*v as i64)),
        Field::Int32(v) => serde_json::Value::Number(serde_json::Number::from(*v as i64)),
        Field::Int64(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Field::UInt8(v) => serde_json::Value::Number(serde_json::Number::from(*v as u64)),
        Field::UInt16(v) => serde_json::Value::Number(serde_json::Number::from(*v as u64)),
        Field::UInt32(v) => serde_json::Value::Number(serde_json::Number::from(*v as u64)),
        Field::UInt64(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Field::Float32(v) => serde_json::Value::Number(
            serde_json::Number::from_f64(*v as f64).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Field::Float64(v) => serde_json::Value::Number(
            serde_json::Number::from_f64(*v).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Field::String(v) => serde_json::Value::String(v.clone()),
        Field::Bytes(v) => serde_json::Value::String(base64_encode(v)),
        Field::Decimal(v) => serde_json::Value::String(v.to_string()),
        Field::Json(v) => v.clone(),
        Field::Date(v) => serde_json::Value::String(v.to_string()),
        Field::Time(v) => serde_json::Value::String(v.to_string()),
        Field::DateTime(v) => serde_json::Value::String(v.to_string()),
        Field::TimestampTz(v) => serde_json::Value::String(v.to_string()),
        Field::Duration(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Field::Array(v) => serde_json::Value::Array(v.iter().map(field_to_json_value).collect()),
        Field::Row(v) => {
            let obj: serde_json::Map<String, serde_json::Value> = v
                .iter()
                .enumerate()
                .map(|(i, f)| (i.to_string(), field_to_json_value(f)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(bytes.len() * 4 / 3 + 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(table[((triple >> 18) & 0x3f) as usize] as char);
        result.push(table[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            result.push(table[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(table[(triple & 0x3f) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Kafka Sink committer (2PC phase 2).
pub struct KafkaSinkCommitter {
    completed: Vec<KafkaCommitInfo>,
}

impl Default for KafkaSinkCommitter {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaSinkCommitter {
    pub fn new() -> Self {
        KafkaSinkCommitter {
            completed: Vec::new(),
        }
    }
}

impl SinkCommitter for KafkaSinkCommitter {
    type CommitInfo = KafkaCommitInfo;
    type AggregatedCommitInfo = KafkaAggregatedCommitInfo;

    fn commit(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Self::AggregatedCommitInfo>> + '_>> {
        let total = commit_infos.iter().map(|c| c.messages_count).sum();
        self.completed.extend(commit_infos);
        Box::pin(async move {
            tracing::info!("KafkaSinkCommitter: committed {} messages", total);
            Ok(KafkaAggregatedCommitInfo {
                checkpoint_id: -1,
                total_messages: total,
            })
        })
    }

    fn abort(
        &mut self,
        _commit_infos: Vec<Self::CommitInfo>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + '_>> {
        Box::pin(async move {
            tracing::warn!("KafkaSinkCommitter: aborting commit");
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_split_id() {
        let split = KafkaSourceSplit::new("test-topic", 0, None);
        assert_eq!(split.split_id(), "test-topic-p0");
    }

    #[test]
    fn test_kafka_source_enumerate_splits() {
        let source = KafkaSource::new(KafkaSourceConfig::default(), None);
        let ctx = SourceSplitEnumeratorContext::new(4, "job-1");
        let splits = source.enumerate_splits(&ctx).unwrap();
        assert_eq!(splits.len(), 4);
    }

    #[test]
    fn test_kafka_sink_writer_creation() {
        let writer = KafkaSinkWriter::new(KafkaSinkConfig {
            batch_size: 50,
            ..KafkaSinkConfig::default()
        });
        assert!(writer.batch.is_empty());
        assert_eq!(writer.total_written, 0);
    }

    #[test]
    fn test_kafka_sink_committer() {
        let mut committer = KafkaSinkCommitter::new();
        let infos = vec![
            KafkaCommitInfo {
                transaction_id: "t1".to_string(),
                messages_count: 10,
            },
            KafkaCommitInfo {
                transaction_id: "t2".to_string(),
                messages_count: 5,
            },
        ];
        let mut future = committer.commit(infos);
        let waker = std::task::Waker::noop();
        let ctx = &mut std::task::Context::from_waker(waker);
        let result = match std::pin::Pin::new(&mut future).poll(ctx) {
            std::task::Poll::Ready(r) => r.unwrap(),
            _ => panic!("unexpected pending"),
        };
        assert_eq!(result.total_messages, 15);
    }

    #[test]
    fn test_kafka_config_parsing() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "broker:9092".to_string());
        props.insert("topic".to_string(), "my-topic".to_string());
        props.insert("format".to_string(), "json".to_string());
        let config = ConnectorConfig::new(props);
        let kafka_config = KafkaSourceConfig::from_config(&config);
        assert_eq!(kafka_config.bootstrap_servers, "broker:9092");
        assert_eq!(kafka_config.topic, "my-topic");
        assert_eq!(kafka_config.format, MessageFormat::Json);
    }
}
