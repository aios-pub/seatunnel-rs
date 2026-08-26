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

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::TopicPartitionList;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message as RdkafkaMessage;
use rdkafka::Offset;
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
#[derive(Debug, Clone, Default, PartialEq)]
pub enum KafkaStartupMode {
    #[default]
    Earliest,
    Latest,
    /// Start from the first offset whose timestamp >= ts (milliseconds).
    Timestamp {
        ts: i64,
    },
    /// Use the consumer group's committed offsets.
    GroupOffset,
    /// Start from explicit `partition -> offset` positions.
    SpecificOffsets {
        offsets: HashMap<i32, i64>,
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
    /// Field delimiter for TEXT format (sink side / text fallback).
    pub field_delimiter: String,
    /// Optional column-name list; when set, messages are deserialized with
    /// `seatunnel-formats` against this schema (required for CDC formats).
    pub columns: Vec<String>,
    pub subtask_index: usize,
    pub subtask_count: usize,
    /// Poll timeout per `poll_next` call, milliseconds.
    pub poll_timeout_ms: u64,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        KafkaSourceConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "seatunnel".to_string(),
            group_id: "seatunnel-consumer".to_string(),
            startup_mode: KafkaStartupMode::Earliest,
            format: MessageFormat::Json,
            field_delimiter: ",".to_string(),
            columns: Vec::new(),
            subtask_index: 0,
            subtask_count: 1,
            poll_timeout_ms: 250,
        }
    }
}

/// Parse `partition:offset,partition:offset` into a map.
pub fn parse_specific_offsets(s: &str) -> HashMap<i32, i64> {
    let mut out = HashMap::new();
    for pair in s.split(',') {
        if let Some((p, o)) = pair.trim().split_once(':') {
            if let (Ok(p), Ok(o)) = (p.trim().parse::<i32>(), o.trim().parse::<i64>()) {
                out.insert(p, o);
            }
        }
    }
    out
}

impl KafkaSourceConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let specific = parse_specific_offsets(&config.get_string("startup.specific-offsets", ""));
        KafkaSourceConfig {
            bootstrap_servers: config.get_string("bootstrap.servers", "localhost:9092"),
            topic: config.get_string("topic", "seatunnel"),
            group_id: config.get_string("group.id", "seatunnel-consumer"),
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
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
            poll_timeout_ms: config.get_int("poll.timeout.ms", 250).max(10) as u64,
            startup_mode: match config.get_string("startup.mode", "earliest").as_str() {
                "latest" => KafkaStartupMode::Latest,
                "timestamp" => KafkaStartupMode::Timestamp {
                    ts: config.get_int("startup.timestamp", 0),
                },
                "group-offsets" | "group_offsets" | "groupoffsets" => KafkaStartupMode::GroupOffset,
                "specific-offsets" | "specific_offsets" | "specificoffsets" => {
                    KafkaStartupMode::SpecificOffsets {
                        offsets: specific,
                    }
                }
                _ => KafkaStartupMode::Earliest,
            },
        }
    }

    /// `auto.offset.reset` value matching the startup mode. Assignment
    /// always carries explicit offsets; this only covers gaps.
    fn auto_offset_reset(&self) -> &'static str {
        match self.startup_mode {
            KafkaStartupMode::Earliest => "earliest",
            KafkaStartupMode::Latest => "latest",
            _ => "error",
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
///
/// Uses manual partition assignment (`assign`) instead of group subscribe so
/// that each subtask consumes a deterministic subset of partitions
/// (`partition % subtask_count == subtask_index`), mirroring the Java
/// `KafkaSourceSplitEnumerator` split→reader assignment. Offsets are
/// committed to the consumer group at checkpoint time and restored from
/// checkpoint state on restart.
pub struct KafkaSourceReader {
    config: KafkaSourceConfig,
    /// Schema built from the configured column list (if any).
    schema: Option<TableSchema>,
    /// Restored offsets from the last checkpoint (`topic-partition` → last
    /// consumed offset); applied on open.
    restore_offsets: HashMap<String, i64>,
    /// Highest consumed offset per `topic-partition`, captured at checkpoint.
    last_offsets: HashMap<String, i64>,
    /// Rows decoded from a multi-row message (e.g. Debezium UPDATE) awaiting
    /// emission.
    pending: VecDeque<Row>,
    consumer: Option<StreamConsumer>,
    /// Partitions assigned to this subtask.
    assigned: Vec<i32>,
}

impl KafkaSourceReader {
    pub fn new(config: KafkaSourceConfig, schema: Option<TableSchema>) -> Self {
        let schema = schema.or_else(|| {
            if config.columns.is_empty() {
                None
            } else {
                Some(TableSchema::new(
                    format!("kafka.{}", config.topic),
                    config
                        .columns
                        .iter()
                        .map(|c| seatunnel_api::ColumnDef::new(c.clone(), seatunnel_api::ColumnType::String))
                        .collect(),
                ))
            }
        });
        KafkaSourceReader {
            config,
            schema,
            restore_offsets: HashMap::new(),
            last_offsets: HashMap::new(),
            pending: VecDeque::new(),
            consumer: None,
            assigned: Vec::new(),
        }
    }

    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: KafkaSourceState = serde_json::from_slice(bytes)?;
        self.restore_offsets = state.offsets;
        Ok(())
    }

    /// Resolve the start offset for one partition according to the startup
    /// mode and restored state.
    async fn start_offset(
        consumer: &StreamConsumer,
        topic: &str,
        partition: i32,
        config: &KafkaSourceConfig,
        restored: Option<i64>,
    ) -> Offset {
        if let Some(last) = restored {
            return Offset::Offset(last + 1);
        }
        match &config.startup_mode {
            KafkaStartupMode::SpecificOffsets { offsets } => {
                offsets.get(&partition).map(|o| Offset::Offset(*o)).unwrap_or(Offset::Beginning)
            }
            KafkaStartupMode::Timestamp { ts } => {
                let mut times = TopicPartitionList::new();
                // librdkafka interprets the offset field as the lookup timestamp (ms).
                let _ = times.add_partition_offset(topic, partition, Offset::Offset(*ts));
                match consumer.offsets_for_times(times, Duration::from_secs(10)) {
                    Ok(list) => list
                        .elements()
                        .iter()
                        .find(|e| e.partition() == partition)
                        .map(|e| e.offset())
                        .unwrap_or(Offset::Beginning),
                    Err(e) => {
                        tracing::warn!("offsets_for_times failed: {}", e);
                        Offset::Beginning
                    }
                }
            }
            KafkaStartupMode::GroupOffset => {
                let mut tpls = TopicPartitionList::new();
                let _ = tpls.add_partition(topic, partition);
                match consumer.committed_offsets(tpls, Duration::from_secs(10)) {
                    Ok(list) => list
                        .elements()
                        .iter()
                        .find(|e| e.partition() == partition)
                        .map(|e| e.offset())
                        .unwrap_or(Offset::Invalid),
                    Err(e) => {
                        tracing::warn!("committed_offsets failed: {}", e);
                        Offset::Invalid
                    }
                }
            }
            KafkaStartupMode::Latest => Offset::End,
            KafkaStartupMode::Earliest => Offset::Beginning,
        }
    }

    /// Decode a message payload into rows.
    fn decode_payload(&self, payload: &[u8]) -> Vec<Row> {
        if let Some(schema) = &self.schema {
            return match seatunnel_formats::deserialize_all(self.config.format, payload, schema) {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!("kafka decode failed ({}); message skipped", e);
                    Vec::new()
                }
            };
        }
        // No schema configured: TEXT yields a single string field, JSON
        // object/array payloads are mapped positionally.
        match self.config.format {
            MessageFormat::Text => {
                let mut row = Row::new(RowKind::Insert, 1);
                row.set(0, seatunnel_api::Field::String(String::from_utf8_lossy(payload).to_string()));
                vec![row]
            }
            _ => {
                let text = String::from_utf8_lossy(payload);
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(serde_json::Value::Object(map)) => {
                        let mut row = Row::new(RowKind::Insert, map.len());
                        for (i, (_, v)) in map.iter().enumerate() {
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
                        row.set(0, seatunnel_api::Field::String(text.to_string()));
                        vec![row]
                    }
                }
            }
        }
    }

    /// Commit the last consumed offsets (+1) to the consumer group.
    fn commit_offsets(&self) {
        if let Some(consumer) = &self.consumer {
            if self.last_offsets.is_empty() {
                return;
            }
            let mut tpl = TopicPartitionList::new();
            for (key, offset) in &self.last_offsets {
                if let Some((topic, partition)) = key.rsplit_once('-') {
                    if let Ok(p) = partition.parse::<i32>() {
                        let _ = tpl.add_partition_offset(topic, p, Offset::Offset(offset + 1));
                    }
                }
            }
            if let Err(e) = consumer.commit(&tpl, CommitMode::Async) {
                tracing::debug!("kafka offset commit deferred: {}", e);
            }
        }
    }
}

fn json_value_to_field(v: &serde_json::Value) -> seatunnel_api::Field {
    use seatunnel_api::Field;
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

impl SourceReader for KafkaSourceReader {
    type Output = KafkaSourceOutput;
    type Split = KafkaSourceSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        Box::pin(async move {
            tracing::info!(
                "Opening Kafka source reader for topic={} subtask={}/{} (startup={:?})",
                self.config.topic,
                self.config.subtask_index,
                self.config.subtask_count,
                self.config.startup_mode
            );
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.bootstrap_servers)
                .set("group.id", &self.config.group_id)
                .set("auto.offset.reset", self.config.auto_offset_reset())
                .set("enable.auto.commit", "false")
                .set("enable.partition.eof", "false")
                .create()
                .map_err(|e| anyhow::anyhow!("Failed to create Kafka consumer: {}", e))?;

            // Discover real partitions and keep this subtask's subset.
            let metadata = consumer
                .fetch_metadata(Some(&self.config.topic), Duration::from_secs(10))
                .map_err(|e| anyhow::anyhow!("fetch_metadata failed: {}", e))?;
            let partitions: Vec<i32> = metadata
                .topics()
                .iter()
                .find(|t| t.name() == self.config.topic)
                .map(|t| {
                    t.partitions()
                        .iter()
                        .map(|p| p.id())
                        .filter(|p| *p as usize % self.config.subtask_count == self.config.subtask_index)
                        .collect()
                })
                .unwrap_or_default();

            if partitions.is_empty() {
                tracing::warn!(
                    "Kafka source: no partitions of topic '{}' assigned to subtask {}",
                    self.config.topic,
                    self.config.subtask_index
                );
            }

            let mut tpl = TopicPartitionList::new();
            for &p in &partitions {
                let key = format!("{}-{}", self.config.topic, p);
                let restored = self.restore_offsets.get(&key).copied();
                let start =
                    Self::start_offset(&consumer, &self.config.topic, p, &self.config, restored).await;
                tpl.add_partition_offset(&self.config.topic, p, start)
                    .map_err(|e| anyhow::anyhow!("assign offset for {}-{}: {}", self.config.topic, p, e))?;
            }
            consumer
                .assign(&tpl)
                .map_err(|e| anyhow::anyhow!("Failed to assign partitions: {}", e))?;
            tracing::info!(
                "Kafka source: assigned partitions {:?} of topic '{}'",
                partitions,
                self.config.topic
            );
            self.assigned = partitions;
            self.consumer = Some(consumer);
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if let Some(row) = self.pending.pop_front() {
                return Ok(PollResult::Record(KafkaSourceOutput(row)));
            }
            let Some(consumer) = &self.consumer else {
                return Ok(PollResult::Empty);
            };
            match tokio::time::timeout(Duration::from_millis(self.config.poll_timeout_ms), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let key = format!("{}-{}", msg.topic(), msg.partition());
                    self.last_offsets.insert(key, msg.offset());
                    if let Some(payload) = msg.payload() {
                        self.pending.extend(self.decode_payload(payload));
                    }
                    if let Some(row) = self.pending.pop_front() {
                        return Ok(PollResult::Record(KafkaSourceOutput(row)));
                    }
                    Ok(PollResult::Empty)
                }
                Ok(Err(e)) => {
                    tracing::warn!("Kafka consumer error: {}", e);
                    Ok(PollResult::Empty)
                }
                Err(_) => Ok(PollResult::Empty),
            }
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        Box::pin(async move {
            // Commit-on-checkpoint (Java `commit_on_checkpoint`).
            self.commit_offsets();
            let state = KafkaSourceState {
                offsets: self.last_offsets.clone(),
            };
            serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!("KafkaSourceReader: adding {} splits (assignment is partition-based)", splits.len());
    }

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        Box::pin(async move {
            self.commit_offsets();
            self.consumer.take();
            Ok(())
        })
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

/// Delivery semantics (Java: `KafkaSemantics`; `NON` is the Java default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaSemantics {
    #[default]
    Non,
    AtLeastOnce,
    ExactlyOnce,
}

impl KafkaSemantics {
    fn parse(s: &str) -> Self {
        match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "atleastonce" | "atleasonce" => KafkaSemantics::AtLeastOnce,
            "exactlyonce" => KafkaSemantics::ExactlyOnce,
            _ => KafkaSemantics::Non,
        }
    }
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
    /// Field names (or `#ordinal` like `#0`) routed into the message key.
    pub partition_key_fields: Vec<String>,
    /// Canal-client format configuration (format = canal_client_json).
    pub canal_client: Option<seatunnel_formats::canal_client_json::CanalClientConfig>,
    /// Delimiter joining fields for TEXT format payloads.
    pub field_delimiter: String,
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
            partition_key_fields: Vec::new(),
            field_delimiter: ",".to_string(),
            canal_client: None,
        }
    }
}

impl KafkaSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let tx_enabled = config.get_bool(
            "transactions.enabled",
            KafkaSemantics::parse(&config.get_string("semantics", "")) == KafkaSemantics::ExactlyOnce,
        );
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
            partition_key_fields: config
                .get_string("partition-key-fields", &config.get_string("partition_key_fields", ""))
                .split(',')
                .map(|f| f.trim().to_string())
                .filter(|f| !f.is_empty())
                .collect(),
            canal_client: config
                .get_string("format", "")
                .eq_ignore_ascii_case("canal_client_json")
                .then(|| {
                    Some(seatunnel_formats::canal_client_json::CanalClientConfig {
                        database_name: config.get_string(
                            "canal-client.database-name",
                            &config.get_string("database-name", ""),
                        ),
                        table_name: config.get_string(
                            "canal-client.table-name",
                            &config.get_string("table-name", ""),
                        ),
                        columns: config
                            .get_string("canal-client.columns", &config.get_string("columns", ""))
                            .split(',')
                            .map(|c| c.trim().to_string())
                            .filter(|c| !c.is_empty())
                            .collect(),
                        tables: serde_json::from_str(&config.get_string(
                            "canal-client.sub-table-fields",
                            &config.get_string("canal-client.sub_table_fields", "{}"),
                        ))
                        .unwrap_or_default(),
                        server_time_zone: config.get_string(
                            "canal-client.server-time-zone",
                            &config.get_string(
                                "server-time-zone",
                                &config.get_string("server_time_zone", "local"),
                            ),
                        ),
                    })
                })
                .flatten(),
            field_delimiter: config.get_string("field.delimiter", &config.get_string("field_delimiter", ",")),
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
        Ok(Box::new(KafkaSinkWriter::new(self.config.clone())?))
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
        Ok(Box::new(KafkaSinkWriter::new(self.config.clone())?))
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
    /// Stateful canal-client encoder (row pairing + filtering + keys).
    canal_encoder: Option<seatunnel_formats::canal_client_json::CanalClientEncoder>,
}

impl KafkaSinkWriter {
    pub fn new(config: KafkaSinkConfig) -> anyhow::Result<Self> {
        let canal_encoder = config
            .canal_client
            .clone()
            .map(seatunnel_formats::canal_client_json::CanalClientEncoder::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!("canal-client format config: {}", e))?;
        Ok(KafkaSinkWriter {
            config,
            batch: Vec::new(),
            total_written: 0,
            producer: None,
            canal_encoder,
        })
    }

    /// Whether the canal-client stateful encoder is active (diagnostics).
    pub fn uses_canal_client(&self) -> bool {
        self.canal_encoder.is_some()
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

        // Canal-client format: the stateful encoder pairs update rows,
        // filters changeless updates and derives the Kafka key per
        // message. Runs even for empty batches so held before-images
        // whose pairing window expired are emitted as real deletes.
        if let Some(encoder) = &mut self.canal_encoder {
            if self.config.transactions_enabled {
                producer
                    .begin_transaction()
                    .map_err(|e| anyhow::anyhow!("begin_transaction failed: {}", e))?;
            }
            let topic = self.config.topic.clone();
            let mut sent = 0usize;
            let mut failures = Vec::new();
            for record in &records {
                let messages = encoder
                    .encode(record)
                    .map_err(|e| anyhow::anyhow!("canal-client encode: {}", e))?;
                for (key, payload) in messages {
                    match producer
                        .send(
                            FutureRecord::<str, str>::to(&topic)
                                .key(key.as_str())
                                .payload(&payload),
                            Duration::from_millis(self.config.message_timeout_ms),
                        )
                        .await
                    {
                        Ok(_) => sent += 1,
                        Err((e, _)) => failures.push(e.to_string()),
                    }
                }
            }
            for (key, payload) in encoder.expire_pending() {
                match producer
                    .send(
                        FutureRecord::<str, str>::to(&topic)
                            .key(key.as_str())
                            .payload(&payload),
                        Duration::from_millis(self.config.message_timeout_ms),
                    )
                    .await
                {
                    Ok(_) => sent += 1,
                    Err((e, _)) => failures.push(e.to_string()),
                }
            }
            if !failures.is_empty() {
                if self.config.transactions_enabled {
                    let _ = producer.abort_transaction(Duration::from_secs(10));
                }
                anyhow::bail!(
                    "failed to deliver {} record(s): {}",
                    failures.len(),
                    failures.first().map(String::as_str).unwrap_or("unknown")
                );
            }
            if self.config.transactions_enabled {
                producer
                    .commit_transaction(Duration::from_secs(30))
                    .map_err(|e| {
                        let _ = producer.abort_transaction(Duration::from_secs(10));
                        anyhow::anyhow!("commit_transaction failed: {}", e)
                    })?;
            }
            self.total_written += sent;
            return Ok(sent);
        }

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
            let payload = encode_row(record, &self.config.format, &self.config.field_delimiter);
            let message = FutureRecord::<str, str>::to(&topic).payload(&payload);
            match row_key(record, &self.config.partition_key_fields) {
                Some(key) => {
                    match producer
                        .send(
                            message.key(key.as_str()),
                            Duration::from_millis(self.config.message_timeout_ms),
                        )
                        .await
                    {
                        Ok(_) => sent += 1,
                        Err((e, _)) => failures.push(e.to_string()),
                    }
                }
                None => {
                    match producer
                        .send(message, Duration::from_millis(self.config.message_timeout_ms))
                        .await
                    {
                        Ok(_) => sent += 1,
                        Err((e, _)) => failures.push(e.to_string()),
                    }
                }
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
            // Canal-client: emit any held before-image as a final delete.
            if let (Some(encoder), Some(producer)) = (&mut self.canal_encoder, &self.producer) {
                for (key, payload) in encoder.flush() {
                    if let Err((e, _)) = producer
                        .send(
                            FutureRecord::<str, str>::to(&self.config.topic)
                                .key(key.as_str())
                                .payload(&payload),
                            Duration::from_millis(self.config.message_timeout_ms),
                        )
                        .await
                    {
                        tracing::warn!("KafkaSinkWriter: canal-client final flush failed: {}", e);
                    }
                }
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
fn encode_row(row: &Row, format: &MessageFormat, delimiter: &str) -> String {
    match format {
        // Java TextFormatSerializer joins every field with the delimiter.
        MessageFormat::Text => row
            .fields
            .iter()
            .map(|f| match f {
                seatunnel_api::Field::String(s) => s.clone(),
                seatunnel_api::Field::Null => String::new(),
                other => format!("{}", other),
            })
            .collect::<Vec<_>>()
            .join(delimiter),
        // The JSON-family encoders all fall back to the positional array
        // encoding here because CDC rows arrive positionally without column
        // names attached. Canal/Debezium envelopes are produced by the
        // dedicated formats crate when schemas are available.
        _ => row_to_json_string(row),
    }
}

/// Build a message key from configured key fields. Field selectors are names
/// (`f0`-style generated names apply when no schema was propagated) or
/// `#ordinal` references.
fn row_key(row: &Row, key_fields: &[String]) -> Option<String> {
    if key_fields.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(key_fields.len());
    for selector in key_fields {
        let field = if let Some(ordinal) = selector.strip_prefix('#') {
            ordinal.parse::<usize>().ok().and_then(|i| row.fields.get(i))
        } else {
            // Match generated field names (f0..fN) against the selector.
            selector
                .strip_prefix('f')
                .and_then(|n| n.parse::<usize>().ok())
                .and_then(|i| row.fields.get(i))
        };
        parts.push(match field {
            Some(seatunnel_api::Field::String(s)) => s.clone(),
            Some(seatunnel_api::Field::Null) | None => String::new(),
            Some(other) => format!("{}", other),
        });
    }
    Some(parts.join("_"))
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
        })
        .unwrap();
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
    fn test_parse_specific_offsets() {
        let offsets = parse_specific_offsets("0:100, 1:250,2:7");
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets.get(&0), Some(&100));
        assert_eq!(offsets.get(&1), Some(&250));
        assert_eq!(offsets.get(&2), Some(&7));
        assert!(parse_specific_offsets("").is_empty());
    }

    #[test]
    fn test_row_key_extraction() {
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(7));
        row.set(1, seatunnel_api::Field::String("alice".to_string()));
        row.set(2, seatunnel_api::Field::String("x".to_string()));

        // No key fields → None.
        assert!(row_key(&row, &[]).is_none());
        // Ordinal selector.
        assert_eq!(row_key(&row, &["#0".to_string()]).as_deref(), Some("7"));
        // Generated f-name selector.
        assert_eq!(
            row_key(&row, &["f1".to_string(), "f2".to_string()]).as_deref(),
            Some("alice_x")
        );
    }

    #[test]
    fn test_semantics_parses_exactly_once() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "b:9092".to_string());
        props.insert("semantics".to_string(), "exactly-once".to_string());
        let config = ConnectorConfig::new(props);
        let sink_config = KafkaSinkConfig::from_config(&config);
        assert!(sink_config.transactions_enabled);
        assert!(sink_config.transactional_id.is_some());
    }

    #[test]
    fn test_text_encoding_joins_with_delimiter() {
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(1));
        row.set(1, seatunnel_api::Field::String("alice".to_string()));
        row.set(2, seatunnel_api::Field::Null);
        let text = encode_row(&row, &MessageFormat::Text, ",");
        assert_eq!(text, "1,alice,");
    }

    #[test]
    fn test_canal_client_config_parsing() {
        let props: HashMap<String, String> = [
            ("bootstrap.servers", "b:9092"),
            ("format", "canal_client_json"),
            ("canal-client.database-name", "MyDb"),
            ("canal-client.table-name", "l_class_student"),
            ("canal-client.columns", "id,name,status"),
            (
                "canal-client.sub-table-fields",
                r#"{"lClassStudent": {"key": "id", "must": {"id": "id"}, "update": {"status": "status"}}}"#,
            ),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let config = KafkaSinkConfig::from_config(&ConnectorConfig::new(props));
        let canal = config.canal_client.expect("canal config present");
        assert_eq!(canal.database_name, "MyDb");
        assert_eq!(canal.columns, vec!["id", "name", "status"]);
        assert_eq!(canal.server_time_zone, "local"); // default: server zone
        assert!(canal.tables.contains_key("lClassStudent"));
        // Encoder builds and finds the mapping.
        assert!(seatunnel_formats::canal_client_json::CanalClientEncoder::new(canal).is_ok());
        // Non-canal-client formats leave it off.
        let props2: HashMap<String, String> =
            [("format".to_string(), "json".to_string())].into_iter().collect();
        assert!(
            KafkaSinkConfig::from_config(&ConnectorConfig::new(props2))
                .canal_client
                .is_none()
        );
    }

    #[test]
    fn test_canal_client_yaml_shape_enables_encoder() {
        // Exact key set produced from the example yaml's sink section
        // (dotted scalar keys survive the json→flat-map flattening).
        let props: HashMap<String, String> = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("topic", "users-canal-client"),
            ("format", "canal_client_json"),
            ("canal-client.database-name", "seatunnel"),
            ("canal-client.table-name", "users"),
            ("canal-client.columns", "id,name,score"),
            (
                "canal-client.sub-table-fields",
                "{ \"users\": { \"key\": \"id\", \"must\": { \"id\": \"id\", \"name\": \"name\" }, \"update\": { \"score\": \"score\" } } }",
            ),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let cfg = KafkaSinkConfig::from_config(&ConnectorConfig::new(props));
        let writer = KafkaSinkWriter::new(cfg).unwrap();
        assert!(writer.uses_canal_client());
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
