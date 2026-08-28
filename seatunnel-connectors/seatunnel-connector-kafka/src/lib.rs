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

use rdkafka::Message as RdkafkaMessage;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use seatunnel_api::{
    row::{Row, RowKind},
    schema::TableSchema,
    sink::{Sink, SinkWriterContext, sink_committer::SinkCommitter, sink_writer::SinkWriter},
    source::{
        Boundedness, Source,
        source_reader::{PollResult, SourceReader, SourceReaderContext},
        source_split::SourceSplit,
        source_split_enum::SourceSplitEnumeratorContext,
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
                    KafkaStartupMode::SpecificOffsets { offsets: specific }
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
    /// Offsets captured at the last `snapshot_state`; committed to the
    /// consumer group only when that checkpoint completes.
    checkpoint_offsets: HashMap<String, i64>,
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
                        .map(|c| {
                            seatunnel_api::ColumnDef::new(
                                c.clone(),
                                seatunnel_api::ColumnType::String,
                            )
                        })
                        .collect(),
                ))
            }
        });
        KafkaSourceReader {
            config,
            schema,
            restore_offsets: HashMap::new(),
            last_offsets: HashMap::new(),
            checkpoint_offsets: HashMap::new(),
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
            KafkaStartupMode::SpecificOffsets { offsets } => offsets
                .get(&partition)
                .map(|o| Offset::Offset(*o))
                .unwrap_or(Offset::Beginning),
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
                row.set(
                    0,
                    seatunnel_api::Field::String(String::from_utf8_lossy(payload).to_string()),
                );
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
        self.commit_specific_offsets(&self.last_offsets);
    }

    fn commit_specific_offsets(&self, offsets: &HashMap<String, i64>) {
        if let Some(consumer) = &self.consumer {
            if offsets.is_empty() {
                return;
            }
            let mut tpl = TopicPartitionList::new();
            for (key, offset) in offsets {
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
                        .filter(|p| {
                            *p as usize % self.config.subtask_count == self.config.subtask_index
                        })
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
                    Self::start_offset(&consumer, &self.config.topic, p, &self.config, restored)
                        .await;
                tpl.add_partition_offset(&self.config.topic, p, start)
                    .map_err(|e| {
                        anyhow::anyhow!("assign offset for {}-{}: {}", self.config.topic, p, e)
                    })?;
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
            match tokio::time::timeout(
                Duration::from_millis(self.config.poll_timeout_ms),
                consumer.recv(),
            )
            .await
            {
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
            // Commit-on-checkpoint (Java `commit_on_checkpoint`): capture
            // now, commit in notify_checkpoint_complete so an aborted
            // checkpoint never advances the consumer group.
            self.checkpoint_offsets = self.last_offsets.clone();
            let state = KafkaSourceState {
                offsets: self.last_offsets.clone(),
            };
            serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    fn notify_checkpoint_complete(
        &mut self,
        checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if !self.checkpoint_offsets.is_empty() {
                tracing::debug!(
                    "KafkaSourceReader: committing consumer offsets for checkpoint {}",
                    checkpoint_id
                );
                self.commit_specific_offsets(&self.checkpoint_offsets);
                self.checkpoint_offsets.clear();
            }
            Ok(())
        })
    }

    fn add_splits(&mut self, splits: Vec<Self::Split>) {
        tracing::info!(
            "KafkaSourceReader: adding {} splits (assignment is partition-based)",
            splits.len()
        );
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
    /// Checkpoint whose transaction this info describes; lets the
    /// committer aggregate per-checkpoint statistics.
    pub checkpoint_id: u64,
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
    /// Max time a partial batch may wait before it is flushed. Without a
    /// linger, records under low traffic would sit in the buffer until the
    /// batch fills (or a checkpoint/EOF flush), which is unbounded latency.
    pub batch_timeout_ms: u64,
    /// When true a transactional producer is used: every checkpoint gets
    /// one Kafka transaction opened right after the previous commit and
    /// committed in `prepare_commit(checkpoint_id)`. Downstream consumers
    /// with `isolation.level=read_committed` never observe partial
    /// checkpoints. The transactional id is stable across restarts
    /// (`{prefix}-{pipeline}-{subtask}`), so a restarted writer fences
    /// (and thereby aborts the open transaction of) any zombie producer
    /// from a previous process run.
    pub transactions_enabled: bool,
    /// Transactional id PREFIX; required when `transactions_enabled` is
    /// true. The full transactional id appends `-{pipeline}-{subtask}`.
    pub transactional_id: Option<String>,
    /// Pipeline name injected by the engine (transaction id namespace).
    pub context_pipeline: String,
    /// Subtask index injected by the engine (transaction id namespace).
    pub context_subtask: usize,
    /// Delivery timeout per record.
    pub message_timeout_ms: u64,
    /// Field names (or `#ordinal` like `#0`) routed into the message key.
    pub partition_key_fields: Vec<String>,
    /// Canal-client format configuration (format = canal_client_json).
    pub canal_client: Option<seatunnel_formats::canal_client_json::CanalClientConfig>,
    /// Delimiter joining fields for TEXT format payloads.
    pub field_delimiter: String,
    /// Ordered per-table topic routes (`topic-routes`): the first entry
    /// whose `pattern` matches the row's origin `database.table` wins;
    /// unmatched rows fall back to `topic`.
    pub topic_routes: Vec<TopicRoute>,
    /// Raw librdkafka producer overrides collected from `producer.*`
    /// sink options (prefix stripped), applied AFTER the built-in
    /// defaults so jobs can re-enable e.g. `producer.linger.ms` batching
    /// or `producer.compression.codec` without code changes.
    pub producer_props: Vec<(String, String)>,
}

/// One `topic-routes` entry.
///
/// `pattern` is an ANCHORED regex over the origin `database.table`
/// identifier (e.g. `shop\.orders_.*`, `shop\.users`); `topic` is the
/// destination and may itself contain the `${database}` / `${table}`
/// placeholders.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TopicRoute {
    pub pattern: String,
    pub topic: String,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        KafkaSinkConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "seatunnel-sink".to_string(),
            format: MessageFormat::Json,
            acks: "all".to_string(),
            batch_size: 100,
            batch_timeout_ms: 100,
            transactions_enabled: false,
            transactional_id: None,
            context_pipeline: "p0".to_string(),
            context_subtask: 0,
            message_timeout_ms: 30_000,
            partition_key_fields: Vec::new(),
            field_delimiter: ",".to_string(),
            canal_client: None,
            topic_routes: Vec::new(),
            producer_props: Vec::new(),
        }
    }
}

impl KafkaSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let tx_enabled = config.get_bool(
            "transactions.enabled",
            KafkaSemantics::parse(&config.get_string("semantics", ""))
                == KafkaSemantics::ExactlyOnce,
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
            batch_timeout_ms: config
                .get_int("batch.timeout.ms", config.get_int("linger.ms", 100))
                .max(0) as u64,
            transactions_enabled: tx_enabled,
            transactional_id: config.get("transactional.id").cloned().or_else(|| {
                if tx_enabled {
                    Some(format!(
                        "seatunnel-{}",
                        config.get_string("job.id", "local")
                    ))
                } else {
                    None
                }
            }),
            context_pipeline: config.get_string("pipeline.name", "p0"),
            context_subtask: config.get_int("subtask.index", 0).max(0) as usize,
            message_timeout_ms: config.get_int("message.timeout.ms", 30_000) as u64,
            partition_key_fields: config
                .get_string(
                    "partition-key-fields",
                    &config.get_string("partition_key_fields", ""),
                )
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
            field_delimiter: config.get_string(
                "field.delimiter",
                &config.get_string("field_delimiter", ","),
            ),
            topic_routes: parse_topic_routes(config),
            producer_props: {
                let mut props: Vec<(String, String)> = config
                    .to_hashmap()
                    .into_iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix("producer.")
                            .map(|name| (name.to_string(), value))
                    })
                    .collect();
                props.sort();
                props
            },
        }
    }
}

/// Parse `topic-routes` / `topic_routes` as a JSON ARRAY of
/// `{"pattern": ..., "topic": ...}` objects (array form keeps the
/// declaration order, which decides the delivery order of multi-topic
/// fan-out; every matching entry receives a copy).
fn parse_topic_routes(config: &ConnectorConfig) -> Vec<TopicRoute> {
    let raw = config.get_string("topic-routes", &config.get_string("topic_routes", ""));
    if raw.trim().is_empty() {
        return Vec::new();
    }
    match serde_json::from_str(&raw) {
        Ok(routes) => routes,
        Err(e) => {
            tracing::warn!("Kafka sink: ignoring invalid topic-routes ({}): {}", raw, e);
            Vec::new()
        }
    }
}

/// Resolves the destination topic(s) per record.
///
/// ALL `topic-routes` entries whose anchored regex matches the record's
/// origin `database.table` identifier receive a copy of the message
/// (mirroring the Java `table_topic_mappings` fan-out: a table listed
/// under several topics is delivered to each); rendered duplicates are
/// collapsed. Records matching no route fall back to the `topic` config
/// value, which may contain the `${database}` / `${table}` placeholders.
/// A plain `topic` without placeholders and without routes keeps every
/// record on one topic (the historical behavior) — non-overlapping
/// route sets behave exactly as before.
struct TopicRouter {
    routes: Vec<(regex::Regex, String)>,
    default_topic: String,
    /// Resolved destinations per origin table. Routes are static after
    /// construction, so entries never expire; the map is bounded by the
    /// number of distinct tables the source emits. Without it every single
    /// record would re-run the full route regex list and re-render the
    /// topic strings.
    cache: HashMap<String, std::sync::Arc<Vec<String>>>,
}

impl TopicRouter {
    fn new(default_topic: &str, routes: &[TopicRoute]) -> anyhow::Result<Self> {
        let compiled = routes
            .iter()
            .map(|route| {
                // Anchored so `shop\.users` matches exactly that table
                // while `shop\.orders_.*` groups a prefix.
                let regex =
                    regex::Regex::new(&format!("^(?s:{})$", route.pattern)).map_err(|e| {
                        anyhow::anyhow!(
                            "topic-routes pattern '{}' is not a valid regex: {}",
                            route.pattern,
                            e
                        )
                    })?;
                Ok((regex, route.topic.clone()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(TopicRouter {
            routes: compiled,
            default_topic: default_topic.to_string(),
            cache: HashMap::new(),
        })
    }

    /// Resolve every destination topic for one record; `origin_table`
    /// is the `database.table` identifier when the source carried one.
    /// Results per table are cached (the mapping is static); the shared
    /// `Arc` clone is a refcount bump on the hot path.
    fn resolve_all(
        &mut self,
        origin_table: Option<&str>,
    ) -> anyhow::Result<std::sync::Arc<Vec<String>>> {
        if let Some(identifier) = origin_table {
            if let Some(cached) = self.cache.get(identifier) {
                return Ok(std::sync::Arc::clone(cached));
            }
            let resolved = std::sync::Arc::new(self.resolve_uncached(origin_table)?);
            self.cache
                .insert(identifier.to_string(), std::sync::Arc::clone(&resolved));
            return Ok(resolved);
        }
        // Rows without an origin table never hit the cache path; they are
        // rare (non-CDC sources) and resolve to the static default topic.
        Ok(std::sync::Arc::new(self.resolve_uncached(origin_table)?))
    }

    fn resolve_uncached(&self, origin_table: Option<&str>) -> anyhow::Result<Vec<String>> {
        let mut topics = Vec::new();
        if let Some(identifier) = origin_table {
            for (regex, topic) in &self.routes {
                if regex.is_match(identifier) {
                    let rendered = render_topic(topic, identifier)?;
                    if !topics.contains(&rendered) {
                        topics.push(rendered);
                    }
                }
            }
            if topics.len() > 1 {
                tracing::debug!(
                    "Kafka sink: table '{}' fans out to {} topics {:?}",
                    identifier,
                    topics.len(),
                    topics
                );
            }
            if !topics.is_empty() {
                return Ok(topics);
            }
        }
        if !topic_has_placeholders(&self.default_topic) {
            return Ok(vec![self.default_topic.clone()]);
        }
        match origin_table {
            Some(identifier) => Ok(vec![render_topic(&self.default_topic, identifier)?]),
            None => anyhow::bail!(
                "topic '{}' contains ${{table}}/${{database}} placeholders but the row has \
                 no origin table — only CDC sources (MySQL-CDC) support per-table routing",
                self.default_topic
            ),
        }
    }
}

fn topic_has_placeholders(topic: &str) -> bool {
    topic.contains("${table}") || topic.contains("${database}")
}

/// Replace the `${database}` / `${table}` placeholders of an identifier
/// split at the LAST dot (a dot-less identifier degrades to an empty
/// database).
fn render_topic(topic: &str, identifier: &str) -> anyhow::Result<String> {
    let (database, table) = match identifier.rsplit_once('.') {
        Some((database, table)) => (database, table),
        None => ("", identifier),
    };
    if topic_has_placeholders(topic) && (database.is_empty() || table.is_empty()) {
        anyhow::bail!(
            "topic '{}' requires the ${{database}}/${{table}} placeholders but origin \
             '{}' does not carry both parts",
            topic,
            identifier
        );
    }
    Ok(topic
        .replace("${database}", database)
        .replace("${table}", table))
}

/// One enqueued delivery awaiting its broker report. The rdkafka
/// `send()` call has ALREADY queued the message with the producer's
/// background thread — this future only observes the delivery report.
struct InFlightDelivery {
    topic: String,
    key: String,
    payload_head: String,
    enqueued_at: std::time::Instant,
    future: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>,
}

/// Bounded FIFO of undelivered messages.
///
/// Flush paths only ENQUEUE (`push`) and return immediately, so the
/// task loop keeps reading the source while librdkafka's background
/// thread batches and delivers — the read/encode/deliver stages fully
/// overlap. Backpressure: [`Self::reap_to_limit`] awaits the OLDEST
/// futures until the queue is back under [`IN_FLIGHT_LIMIT`], keeping
/// memory bounded. [`Self::drain`] awaits everything (checkpoint /
/// close barriers — at-least-once delivery is preserved because the
/// checkpoint cannot advance past undelivered messages).
struct InFlightDeliveries {
    pending: std::collections::VecDeque<InFlightDelivery>,
}

/// Max undelivered messages buffered before flush blocks on the oldest
/// (~8× the default flush batch; each entry is small — the payload
/// itself is owned by librdkafka's queue).
const IN_FLIGHT_LIMIT: usize = 8192;

/// Detail prefix length kept from a payload for error logs.
const PAYLOAD_HEAD_CHARS: usize = 200;

impl InFlightDeliveries {
    fn new() -> Self {
        InFlightDeliveries {
            pending: std::collections::VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    /// Register a delivery future (the message is already queued with
    /// the producer; the future resolves on the delivery report).
    /// Production paths use [`Self::push_delivery`]; this is the
    /// synthetic-future entry point for tests.
    #[cfg(test)]
    fn push(
        &mut self,
        topic: &str,
        key: &str,
        payload: &str,
        future: impl std::future::Future<Output = Result<(), String>> + Send + 'static,
    ) {
        self.pending.push_back(InFlightDelivery {
            topic: topic.to_string(),
            key: key.to_string(),
            payload_head: payload.chars().take(PAYLOAD_HEAD_CHARS).collect(),
            enqueued_at: std::time::Instant::now(),
            future: Box::pin(future),
        });
    }

    /// Enqueue one owned message for delivery. The rdkafka delivery
    /// future borrows its record, so the record moves into the future's
    /// closure (making it 'static) and the `send` itself happens inside
    /// the closure — calling this does NOT block on the broker.
    fn push_delivery(
        &mut self,
        producer: FutureProducer,
        topic: String,
        key: String,
        payload: String,
        timeout: Duration,
    ) {
        let payload_head: String = payload.chars().take(PAYLOAD_HEAD_CHARS).collect();
        let future = {
            let topic = topic.clone();
            let key = key.clone();
            async move {
                producer
                    .send(
                        FutureRecord::<str, str>::to(&topic)
                            .key(&key)
                            .payload(&payload),
                        timeout,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|(e, _)| e.to_string())
            }
        };
        self.pending.push_back(InFlightDelivery {
            topic,
            key,
            payload_head,
            enqueued_at: std::time::Instant::now(),
            future: Box::pin(future),
        });
    }

    /// Await futures (oldest first) collecting outcomes into the
    /// metrics; returns (delivered, failed, first-few error details).
    async fn reap(
        &mut self,
        take: usize,
        metrics: &seatunnel_api::sink::SinkMetrics,
    ) -> (u64, u64, Vec<String>) {
        let mut delivered = 0u64;
        let mut failed = 0u64;
        let mut latencies = Vec::new();
        let mut errors = Vec::new();
        let mut last_error: Option<String> = None;
        for _ in 0..take.min(self.pending.len()) {
            let Some(delivery) = self.pending.pop_front() else {
                break;
            };
            let latency_ms = delivery.enqueued_at.elapsed().as_millis() as u64;
            match delivery.future.await {
                Ok(()) => {
                    delivered += 1;
                    latencies.push(latency_ms);
                }
                Err(error) => {
                    failed += 1;
                    // Detailed per-delivery error log: full rdkafka error
                    // plus routing context and the payload prefix.
                    tracing::error!(
                        topic = %delivery.topic,
                        key = %delivery.key,
                        elapsed_ms = latency_ms,
                        error = %error,
                        payload_head = %delivery.payload_head,
                        "kafka delivery failed"
                    );
                    let detail = format!(
                        "{} (topic={}, key={}, after {}ms, payload~'{}')",
                        error, delivery.topic, delivery.key, latency_ms, delivery.payload_head
                    );
                    if last_error.is_none() {
                        last_error = Some(detail.clone());
                    }
                    if errors.len() < 3 {
                        errors.push(detail);
                    }
                }
            }
        }
        metrics.record_deliveries(delivered, failed, &latencies, last_error.as_deref());
        (delivered, failed, errors)
    }

    /// Non-blocking sweep over already-resolved futures (polled with
    /// `now_or_never`, never parks the write loop). Without it, outcomes
    /// are only observed at the backpressure/drain barriers — under low
    /// outstanding counts that means the CHECKPOINT boundary, so the
    /// windowed delivery metrics would report checkpoint cadence instead
    /// of the real broker latency.
    fn reap_completed(&mut self, metrics: &seatunnel_api::sink::SinkMetrics) {
        use futures::FutureExt;
        if self.pending.is_empty() {
            return;
        }
        let mut delivered = 0u64;
        let mut failed = 0u64;
        let mut latencies = Vec::new();
        let mut last_error: Option<String> = None;
        let mut retained = VecDeque::with_capacity(self.pending.len());
        while let Some(mut delivery) = self.pending.pop_front() {
            let latency_ms = delivery.enqueued_at.elapsed().as_millis() as u64;
            // `&mut` (always Unpin) lets now_or_never poll without
            // consuming the boxed future, so unresolved deliveries can
            // be retained untouched.
            match (&mut delivery.future).now_or_never() {
                Some(Ok(())) => {
                    delivered += 1;
                    latencies.push(latency_ms);
                }
                Some(Err(error)) => {
                    failed += 1;
                    tracing::error!(
                        topic = %delivery.topic,
                        key = %delivery.key,
                        elapsed_ms = latency_ms,
                        error = %error,
                        payload_head = %delivery.payload_head,
                        "kafka delivery failed"
                    );
                    if last_error.is_none() {
                        last_error = Some(format!(
                            "{} (topic={}, key={}, after {}ms, payload~'{}')",
                            error, delivery.topic, delivery.key, latency_ms, delivery.payload_head
                        ));
                    }
                }
                None => retained.push_back(delivery),
            }
        }
        self.pending = retained;
        metrics.record_deliveries(delivered, failed, &latencies, last_error.as_deref());
    }

    /// Backpressure barrier: await the oldest futures until the queue
    /// is back under [`IN_FLIGHT_LIMIT`]. Delivery failures surface as
    /// an aggregated error (task restart replays from the checkpoint).
    async fn reap_to_limit(
        &mut self,
        metrics: &seatunnel_api::sink::SinkMetrics,
    ) -> anyhow::Result<()> {
        if self.len() < IN_FLIGHT_LIMIT {
            return Ok(());
        }
        let excess = self.len() - IN_FLIGHT_LIMIT + 1;
        let (_, failed, errors) = self.reap(excess, metrics).await;
        if failed > 0 {
            anyhow::bail!(
                "failed to deliver {} record(s); first errors: [{}]",
                failed,
                errors.join(" | ")
            );
        }
        Ok(())
    }

    /// Full barrier: await every undelivered message (checkpoint /
    /// close). Returns the delivered count.
    async fn drain(&mut self, metrics: &seatunnel_api::sink::SinkMetrics) -> anyhow::Result<u64> {
        let total = self.len();
        if total == 0 {
            return Ok(0);
        }
        let (delivered, failed, errors) = self.reap(total, metrics).await;
        if failed > 0 {
            anyhow::bail!(
                "failed to deliver {}/{} record(s); first errors: [{}]",
                failed,
                total,
                errors.join(" | ")
            );
        }
        Ok(delivered)
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
        ctx: &SinkWriterContext,
    ) -> anyhow::Result<
        Box<
            dyn SinkWriter<
                    Input = Self::Input,
                    WriterState = Self::WriterState,
                    CommitInfo = Self::CommitInfo,
                >,
        >,
    > {
        Ok(Box::new(KafkaSinkWriter::new(
            self.config.clone(),
            ctx.metrics.clone(),
        )?))
    }

    fn restore_writer(
        &self,
        ctx: &SinkWriterContext,
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
        let mut writer = KafkaSinkWriter::new(self.config.clone(), ctx.metrics.clone())?;
        if let Some(bytes) = states.last() {
            writer.restore_from_state_bytes(bytes)?;
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
/// Records are buffered by `write()` and delivered when the batch reaches
/// `batch_size`, when the `batch.timeout.ms` linger elapses, or at
/// `prepare_commit()` (checkpoint boundary).
///
/// With `transactions.enabled=true` exactly one Kafka transaction spans each
/// checkpoint window: `open()` initializes the transactional producer
/// (fencing any zombie from a previous run, which aborts its hanging
/// transaction) and begins the first transaction; every flush only DELIVERS
/// records into the open transaction; `prepare_commit(checkpoint_id)`
/// commits it and immediately begins the next one. `read_committed`
/// consumers therefore observe checkpoint windows atomically and never see
/// partial batches.
pub struct KafkaSinkWriter {
    config: KafkaSinkConfig,
    batch: Vec<Row>,
    total_written: usize,
    producer: Option<FutureProducer>,
    /// When the last flush happened; drives the `batch.timeout.ms` linger.
    last_flush: std::time::Instant,
    /// Stateful canal-client encoder (row pairing + filtering + keys).
    /// Explicit `canal-client.columns` builds a single-table encoder
    /// eagerly; otherwise the encoder starts EMPTY and registers one
    /// state per table as the initial-schema events arrive (see
    /// `apply_schema_change`).
    canal_encoder: Option<seatunnel_formats::canal_client_json::CanalClientEncoder>,
    /// Per-record topic resolution (`topic-routes` + `topic` template).
    topic_router: TopicRouter,
    /// Undelivered messages; flush enqueues without blocking, the
    /// checkpoint/close barriers drain (see [`InFlightDeliveries`]).
    in_flight: InFlightDeliveries,
    /// Windowed delivery metrics shared with the task layer.
    metrics: std::sync::Arc<seatunnel_api::sink::SinkMetrics>,
    /// Full transactional id (`{prefix}-{pipeline}-{subtask}`); stable
    /// across restarts of the same job so zombies can be fenced.
    txn_base: Option<String>,
    /// Whether a transaction is currently open (begin issued, not committed).
    txn_open: bool,
    /// Messages delivered inside the currently open transaction.
    txn_messages: usize,
    /// Last checkpoint id whose transaction this writer committed.
    last_committed_checkpoint: u64,
}

impl KafkaSinkWriter {
    pub fn new(
        config: KafkaSinkConfig,
        metrics: std::sync::Arc<seatunnel_api::sink::SinkMetrics>,
    ) -> anyhow::Result<Self> {
        let topic_router = TopicRouter::new(&config.topic, &config.topic_routes)?;
        // Explicit canal-client config (columns + sub-table-fields entry)
        // builds the single-table encoder eagerly and fails fast on a
        // missing mapping. Without `canal-client.columns` the encoder
        // starts empty and registers per-table states from the source's
        // initial-schema events (automatic column mapping).
        let canal_encoder = match &config.canal_client {
            Some(canal) if !canal.columns.is_empty() => Some(
                seatunnel_formats::canal_client_json::CanalClientEncoder::new(canal.clone())
                    .map_err(|e| anyhow::anyhow!("canal-client format config: {}", e))?,
            ),
            Some(canal) => Some(
                seatunnel_formats::canal_client_json::CanalClientEncoder::new_auto(canal.clone()),
            ),
            None => None,
        };
        let txn_base = config.transactions_enabled.then(|| {
            format!(
                "{}-{}-{}",
                config
                    .transactional_id
                    .clone()
                    .unwrap_or_else(|| "seatunnel-local".to_string()),
                config.context_pipeline,
                config.context_subtask
            )
        });
        Ok(KafkaSinkWriter {
            config,
            batch: Vec::new(),
            total_written: 0,
            producer: None,
            last_flush: std::time::Instant::now(),
            canal_encoder,
            topic_router,
            in_flight: InFlightDeliveries::new(),
            metrics,
            txn_base,
            txn_open: false,
            txn_messages: 0,
            last_committed_checkpoint: 0,
        })
    }

    /// Register one table's schema in the schema-driven canal-client
    /// encoder. Explicit encoders ignore registrations (their static
    /// column list stays authoritative); replayed events are idempotent.
    fn register_canal_schema(&mut self, schema: &seatunnel_api::TableSchema) -> anyhow::Result<()> {
        let Some(encoder) = &mut self.canal_encoder else {
            return Ok(());
        };
        if encoder.is_explicit() {
            return Ok(());
        }
        let already_registered = encoder.registered_tables();
        encoder
            .register_schema(schema)
            .map_err(|e| anyhow::anyhow!("canal-client auto mapping: {}", e))?;
        if encoder.registered_tables() != already_registered {
            tracing::info!(
                "KafkaSinkWriter: canal-client auto mapping registered schema '{}' \
                 ({} columns, {} table(s) total)",
                schema.table_identifier,
                schema.columns.len(),
                encoder.registered_tables()
            );
        }
        Ok(())
    }

    /// Restore the writer's checkpoint progress (last committed checkpoint
    /// and totals) from a serialized `snapshot_state` payload. The next
    /// `open()` re-initializes the same transactional id, which fences any
    /// zombie producer left by the crashed run.
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|e| anyhow::anyhow!("kafka writer state: {}", e))?;
        self.total_written = state
            .get("total_written")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        self.last_committed_checkpoint = state
            .get("last_committed_checkpoint")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        tracing::info!(
            "KafkaSinkWriter: restored state (last committed checkpoint {}, total written {})",
            self.last_committed_checkpoint,
            self.total_written
        );
        Ok(())
    }

    /// Whether the canal-client stateful encoder is active.
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
            .set("acks", &self.config.acks)
            // librdkafka's default linger (5ms) interacts badly with
            // proxied/NAT'd broker links (e.g. docker port-forwarding),
            // degrading pipelined produce to ~1 message per linger window.
            // Batch without the artificial delay and let TCP stack coalesce.
            .set("linger.ms", "0")
            .set("socket.nagle.disable", "true");
        // `producer.*` overrides come last so they win over the defaults
        // above — e.g. `producer.linger.ms: 5` re-enables librdkafka
        // batching on links that tolerate it (with the in-flight delivery
        // pipeline the task loop no longer blocks per message).
        for (name, value) in &self.config.producer_props {
            tracing::debug!("Kafka sink: producer override {}={}", name, value);
            builder.set(name, value);
        }
        if let Some(txn_base) = &self.txn_base {
            tracing::info!("Kafka sink: transactional producer id={}", txn_base);
            builder.set("transactional.id", txn_base);
        }
        let producer: FutureProducer = builder
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka producer: {}", e))?;
        if self.txn_base.is_some() {
            // init_transactions on the stable transactional id bumps the
            // epoch: any producer instance from a previous (crashed) run
            // is fenced and its hanging transaction aborted by the
            // transaction coordinator.
            producer
                .init_transactions(std::time::Duration::from_secs(30))
                .map_err(|e| anyhow::anyhow!("init_transactions failed: {}", e))?;
        }
        self.producer = Some(producer);
        Ok(())
    }

    /// Resolve the destination topic(s) for one encoded message and
    /// enqueue a delivery to each (fan-out copies share the same
    /// payload/requestId). The owned key and payload move into the LAST
    /// delivery — single-topic delivery (the common case) never clones
    /// the payload, earlier fan-out copies clone once per extra topic.
    /// Returns the number of deliveries enqueued.
    fn fanout_delivery(
        router: &mut TopicRouter,
        in_flight: &mut InFlightDeliveries,
        producer: &FutureProducer,
        origin: Option<&str>,
        key: String,
        payload: String,
        timeout: Duration,
    ) -> anyhow::Result<usize> {
        let topics = router
            .resolve_all(origin)
            .map_err(|e| anyhow::anyhow!("topic routing: {}", e))?;
        let last_idx = topics.len().saturating_sub(1);
        let mut key = Some(key);
        let mut payload = Some(payload);
        for (i, topic) in topics.iter().enumerate() {
            let is_last = i == last_idx;
            let key = if is_last {
                key.take().unwrap()
            } else {
                key.as_ref().unwrap().clone()
            };
            let payload = if is_last {
                payload.take().unwrap()
            } else {
                payload.as_ref().unwrap().clone()
            };
            in_flight.push_delivery(producer.clone(), topic.clone(), key, payload, timeout);
        }
        Ok(topics.len())
    }

    /// Deliver buffered records to Kafka. With transactions enabled the
    /// records are produced into the currently OPEN transaction but never
    /// committed here — only `prepare_commit` (checkpoint boundary) commits.
    async fn flush_batch(&mut self) -> anyhow::Result<usize> {
        self.last_flush = std::time::Instant::now();
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
            if encoder.registered_tables() == 0 && !records.is_empty() {
                // Automatic mapping still waiting for every table's
                // initial-schema event; reaching here with buffered rows
                // means the source emitted data before its schema (or
                // never emitted one).
                anyhow::bail!(
                    "canal-client automatic column mapping: {} row(s) arrived before any \
                     initial schema event — the source must emit the table schema first \
                     (MySQL-CDC does; or configure canal-client.columns explicitly)",
                    records.len()
                );
            }
            let mut sent = 0usize;
            let timeout = Duration::from_millis(self.config.message_timeout_ms);
            for record in &records {
                let messages = encoder
                    .encode(record)
                    .map_err(|e| anyhow::anyhow!("canal-client encode: {}", e))?;
                for message in messages {
                    // Multi-topic fan-out: every matching route gets the
                    // SAME encoded message (same requestId — a usable
                    // idempotency key across topics).
                    sent += Self::fanout_delivery(
                        &mut self.topic_router,
                        &mut self.in_flight,
                        &producer,
                        Some(message.table.as_str()),
                        message.key,
                        message.payload,
                        timeout,
                    )?;
                }
            }
            for message in encoder.expire_pending() {
                sent += Self::fanout_delivery(
                    &mut self.topic_router,
                    &mut self.in_flight,
                    &producer,
                    Some(message.table.as_str()),
                    message.key,
                    message.payload,
                    timeout,
                )?;
            }
            self.metrics.record_sent(sent as u64);
            self.total_written += sent;
            self.txn_messages += sent;
            // Non-blocking sweep of resolved futures keeps the
            // windowed metrics at real broker latency, then bounded
            // backpressure (normally returns immediately, letting the
            // source keep reading while delivery overlaps).
            self.in_flight.reap_completed(&self.metrics);
            self.in_flight.reap_to_limit(&self.metrics).await?;
            return Ok(sent);
        }

        if records.is_empty() {
            return Ok(0);
        }

        let timeout = Duration::from_millis(self.config.message_timeout_ms);
        let mut sent = 0usize;
        let payloads: Vec<String> = records
            .iter()
            .map(|record| encode_row(record, &self.config.format, &self.config.field_delimiter))
            .collect();
        let keys: Vec<Option<String>> = records
            .iter()
            .map(|record| row_key(record, &self.config.partition_key_fields))
            .collect();
        // Rows carrying an origin table (CDC sources) resolve their
        // topic(s) per record — possibly several when overlapping routes
        // fan out; rows without one share the static topic.
        //
        // Enqueue every record (librdkafka batches in the background);
        // the delivery futures are observed at the reap/drain barriers,
        // so the source keeps reading while delivery overlaps.
        for ((payload, key), record) in payloads
            .into_iter()
            .zip(keys.into_iter())
            .zip(records.iter())
        {
            sent += Self::fanout_delivery(
                &mut self.topic_router,
                &mut self.in_flight,
                &producer,
                record.origin_table.as_deref(),
                key.unwrap_or_default(),
                payload,
                timeout,
            )?;
        }
        self.metrics.record_sent(sent as u64);
        self.total_written += sent;
        self.txn_messages += sent;
        // Non-blocking sweep of resolved futures keeps the windowed
        // metrics at real broker latency; bounded backpressure only —
        // normally returns immediately.
        self.in_flight.reap_completed(&self.metrics);
        self.in_flight.reap_to_limit(&self.metrics).await?;
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
            if self.txn_base.is_some() {
                // Open the first checkpoint window's transaction. Any zombie
                // transaction from a previous run was already aborted by
                // init_transactions inside ensure_producer.
                if let Some(producer) = &self.producer {
                    producer
                        .begin_transaction()
                        .map_err(|e| anyhow::anyhow!("begin_transaction failed: {}", e))?;
                    self.txn_open = true;
                }
            }
            tracing::info!(
                "KafkaSinkWriter: producer ready for topic {} via {} (txn={:?}, last_committed_cp={})",
                self.config.topic,
                self.config.bootstrap_servers,
                self.txn_base,
                self.last_committed_checkpoint
            );
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Send + '_>> {
        // Buffer the record; delivery happens at batch size, when the
        // `batch.timeout.ms` linger elapses, or at prepare_commit/close.
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

    fn apply_schema_change(
        &mut self,
        event: &seatunnel_api::SchemaChangeEvent,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Canal-client automatic mapping: EVERY initial-schema event
        // registers its table (positional columns, primary key, identity
        // field mapping), so multi-table sources map each table with its
        // own schema. Regular DDL changes are still ignored in auto mode;
        // tracking schema evolution needs the explicit
        // canal-client.columns config.
        if self.config.canal_client.is_some() {
            if let Some(schema) = event.initial_schema_snapshot() {
                let schema = schema.clone();
                return Box::pin(async move { self.register_canal_schema(&schema) });
            }
        }
        Box::pin(async move { Ok(()) })
    }

    fn prepare_commit(
        &mut self,
        checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            let pending = self.batch.len();
            let delivered = self.flush_batch().await?;
            // Checkpoint barrier: every message enqueued before this
            // checkpoint must be DELIVERED before it advances, otherwise
            // a crash right after the commit would lose buffered sends
            // (at-least-once contract). The drained count spans all
            // write-triggered flushes since the previous checkpoint, not
            // just this flush's batch.
            self.in_flight.drain(&self.metrics).await?;
            let mut info = KafkaCommitInfo {
                transaction_id: self
                    .txn_base
                    .clone()
                    .unwrap_or_else(|| format!("non-txn-{}", self.config.topic)),
                checkpoint_id,
                messages_count: delivered,
            };
            if self.txn_base.is_some() {
                let producer = self
                    .producer
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("kafka producer unavailable"))?;
                // Checkpoint boundary: make the whole window visible in one
                // atomic transaction, then immediately open the next one.
                producer
                    .commit_transaction(Duration::from_secs(30))
                    .map_err(|e| {
                        let _ = producer.abort_transaction(Duration::from_secs(10));
                        anyhow::anyhow!("commit_transaction failed: {}", e)
                    })?;
                self.txn_open = false;
                info.messages_count = self.txn_messages;
                self.txn_messages = 0;
                self.last_committed_checkpoint = checkpoint_id;
                producer
                    .begin_transaction()
                    .map_err(|e| anyhow::anyhow!("begin_transaction failed: {}", e))?;
                self.txn_open = true;
            }
            tracing::info!(
                "KafkaSinkWriter: checkpoint {} committed {} message(s) (pending_rows={}, total={})",
                checkpoint_id,
                info.messages_count,
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
            "transactional_base": self.txn_base,
            "last_committed_checkpoint": self.last_committed_checkpoint,
        });
        Box::pin(async move { serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e)) })
    }

    fn poll_flush(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Flush the tail of a partial batch once the linger elapsed; without
        // this, records buffered at the end of a burst wait for the next
        // write or a checkpoint boundary.
        let due = !self.batch.is_empty()
            && self.last_flush.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms);
        Box::pin(async move {
            if due {
                self.flush_batch().await?;
            }
            Ok(())
        })
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
                let timeout = Duration::from_millis(self.config.message_timeout_ms);
                for message in encoder.flush() {
                    if let Err(e) = Self::fanout_delivery(
                        &mut self.topic_router,
                        &mut self.in_flight,
                        producer,
                        Some(message.table.as_str()),
                        message.key,
                        message.payload,
                        timeout,
                    ) {
                        tracing::warn!(
                            "KafkaSinkWriter: canal-client final flush routing failed: {}",
                            e
                        );
                    }
                }
            }
            // Close barrier: wait for every undelivered message.
            if let Err(e) = self.in_flight.drain(&self.metrics).await {
                tracing::warn!("KafkaSinkWriter: final drain failed: {}", e);
            }
            // Transactional mode: the graceful path committed at the final
            // checkpoint; whatever landed in the still-open transaction
            // above (no final checkpoint ran, e.g. after a task error) is
            // committed so it is not stranded invisible. A restore replays
            // from the last persisted checkpoint regardless.
            if self.txn_open {
                if let Some(producer) = &self.producer {
                    match producer.commit_transaction(Duration::from_secs(30)) {
                        Ok(()) => {
                            self.txn_open = false;
                            self.txn_messages = 0;
                        }
                        Err(e) => {
                            let _ = producer.abort_transaction(Duration::from_secs(10));
                            tracing::warn!(
                                "KafkaSinkWriter: final commit_transaction failed: {}",
                                e
                            );
                        }
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
            ordinal
                .parse::<usize>()
                .ok()
                .and_then(|i| row.fields.get(i))
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
    ) -> seatunnel_api::sink::sink_committer::CommitterFuture<'_, Self::AggregatedCommitInfo> {
        let total = commit_infos.iter().map(|c| c.messages_count).sum();
        let checkpoint_id = commit_infos.iter().map(|c| c.checkpoint_id).max();
        self.completed.extend(commit_infos);
        Box::pin(async move {
            tracing::info!(
                "KafkaSinkCommitter: checkpoint {:?} committed {} messages",
                checkpoint_id,
                total
            );
            Ok(KafkaAggregatedCommitInfo {
                checkpoint_id: checkpoint_id.map(|v| v as i64).unwrap_or(-1),
                total_messages: total,
            })
        })
    }

    fn abort(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> seatunnel_api::sink::sink_committer::CommitterFuture<'_, ()> {
        Box::pin(async move {
            // A Kafka transaction is committed inside prepare_commit (phase
            // 1) because rdkafka cannot resume a prepared transaction from
            // another producer instance (Java needs a reflection hack for
            // that). Once committed it cannot be rolled back; an aborted
            // checkpoint replays from the previous one and message keys
            // make the duplicate window idempotent downstream.
            tracing::warn!(
                "KafkaSinkCommitter: checkpoint aborted after {} commit info(s); \
                 transactions already committed at prepare_commit cannot be rolled back",
                commit_infos.len()
            );
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

    fn metrics() -> std::sync::Arc<seatunnel_api::sink::SinkMetrics> {
        std::sync::Arc::new(seatunnel_api::sink::SinkMetrics::new())
    }

    #[test]
    fn test_kafka_sink_writer_creation() {
        let writer = KafkaSinkWriter::new(
            KafkaSinkConfig {
                batch_size: 50,
                ..KafkaSinkConfig::default()
            },
            metrics(),
        )
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
                checkpoint_id: 1,
                messages_count: 10,
            },
            KafkaCommitInfo {
                transaction_id: "t2".to_string(),
                checkpoint_id: 1,
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
        let props2: HashMap<String, String> = [("format".to_string(), "json".to_string())]
            .into_iter()
            .collect();
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
        let writer = KafkaSinkWriter::new(cfg, metrics()).unwrap();
        assert!(writer.uses_canal_client());
    }

    #[test]
    fn test_canal_client_auto_mode_defers_to_schema_event() {
        // No canal-client.columns / sub-table-fields: the encoder starts
        // empty and registers one state per table as the initial-schema
        // events arrive. The producer connects lazily, so this path never
        // contacts a broker in the error case.
        let props: HashMap<String, String> = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("topic", "users-canal-auto"),
            ("format", "canal_client_json"),
            ("canal-client.database-name", "seatunnel"),
            ("canal-client.table-name", "users"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let cfg = KafkaSinkConfig::from_config(&ConnectorConfig::new(props));
        let mut writer = KafkaSinkWriter::new(cfg, metrics()).unwrap();
        assert!(writer.uses_canal_client());
        assert_eq!(
            writer.canal_encoder.as_ref().unwrap().registered_tables(),
            0,
            "auto mode starts with no table registered"
        );

        // Data before the schema event must fail loudly, not encode as
        // plain JSON.
        writer.batch.push({
            let mut row = Row::new(RowKind::Insert, 3);
            row.set(0, seatunnel_api::Field::Int64(1));
            row.set(1, seatunnel_api::Field::String("alice".into()));
            row.set(2, seatunnel_api::Field::Int64(90));
            row
        });
        let flushed = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(writer.flush_batch());
        assert!(flushed.is_err(), "rows before the schema event must error");
        assert!(
            writer.batch.is_empty(),
            "failed flush still drains the batch"
        );

        // The initial-schema event configures the automatic mapping:
        // all columns identity-mapped, primary key as partition key.
        let schema = seatunnel_api::TableSchema::new(
            "seatunnel.users",
            vec![
                seatunnel_api::ColumnDef::new("id", seatunnel_api::ColumnType::Int64).primary_key(),
                seatunnel_api::ColumnDef::new("name", seatunnel_api::ColumnType::String),
                seatunnel_api::ColumnDef::new("score", seatunnel_api::ColumnType::Int64),
            ],
        );
        let event = seatunnel_api::SchemaChangeEvent::initial_schema(schema);
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(writer.apply_schema_change(&event))
            .unwrap();
        assert_eq!(
            writer.canal_encoder.as_ref().unwrap().registered_tables(),
            1
        );
        assert_eq!(writer.canal_encoder.as_ref().unwrap().fields().key, "id");

        // A row without origin identity falls back to the first
        // registered table (single-table backward compatibility) and
        // encodes as a canal-client insert message.
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(1));
        row.set(1, seatunnel_api::Field::String("alice".into()));
        row.set(2, seatunnel_api::Field::Int64(90));
        let mut encoder = writer.canal_encoder.take().expect("encoder configured");
        let messages = encoder.encode(&row).unwrap();
        writer.canal_encoder = Some(encoder);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, "1");
        let payload: serde_json::Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(payload["dbName"], "seatunnel");
        assert_eq!(payload["tableName"], "users");
        assert_eq!(payload["eventType"], "insert");
        assert_eq!(payload["data"]["name"], "alice");
        assert_eq!(payload["data"]["score"], 90);

        // A later initial event REGISTERS its own table: multi-table
        // sources map each table with its own schema.
        let schema2 = seatunnel_api::TableSchema::new(
            "seatunnel.users_2",
            vec![
                seatunnel_api::ColumnDef::new("id", seatunnel_api::ColumnType::Int64).primary_key(),
                seatunnel_api::ColumnDef::new("other", seatunnel_api::ColumnType::String),
            ],
        );
        let event2 = seatunnel_api::SchemaChangeEvent::initial_schema(schema2);
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(writer.apply_schema_change(&event2))
            .unwrap();
        let encoder = writer.canal_encoder.as_ref().unwrap();
        assert_eq!(encoder.registered_tables(), 2, "every schema registers");
        assert_eq!(
            encoder.fields().must.len(),
            3,
            "the default (first) table keeps its mapping"
        );

        // A row tagged with the second table encodes against ITS schema.
        let mut row2 = Row::new(RowKind::Insert, 2);
        row2.set(0, seatunnel_api::Field::Int64(7));
        row2.set(1, seatunnel_api::Field::String("bob".into()));
        row2.origin_table = Some("seatunnel.users_2".to_string());
        let mut encoder = writer.canal_encoder.take().unwrap();
        let messages = encoder.encode(&row2).unwrap();
        writer.canal_encoder = Some(encoder);
        assert_eq!(messages.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(payload["tableName"], "users_2");
        assert_eq!(payload["data"]["other"], "bob");
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

    #[test]
    fn test_topic_router_literal_topic_is_static() {
        let mut router = TopicRouter::new("plain-topic", &[]).unwrap();
        assert_eq!(*router.resolve_all(None).unwrap(), vec!["plain-topic"]);
        assert_eq!(
            (*router.resolve_all(Some("shop.orders")).unwrap()),
            vec!["plain-topic"],
            "a literal topic ignores row origins"
        );
    }

    #[test]
    fn test_topic_router_caches_resolved_topics_per_table() {
        let routes: Vec<TopicRoute> = serde_json::from_str(
            r#"[
                {"pattern": "shop\\.orders", "topic": "topic_orders"},
                {"pattern": "shop\\..*", "topic": "log_${table}"}
            ]"#,
        )
        .unwrap();
        let mut router = TopicRouter::new("fallback", &routes).unwrap();
        let first = router.resolve_all(Some("shop.orders")).unwrap();
        assert_eq!(*first, vec!["topic_orders", "log_orders"]);
        let second = router.resolve_all(Some("shop.orders")).unwrap();
        // Cache hit: same allocation, not a re-resolve through the regexes.
        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "repeated tables must resolve through the cache"
        );
        assert_eq!(router.cache.len(), 1, "only the seen table is cached");
        // A different table resolves independently.
        assert_eq!(
            (*router.resolve_all(Some("shop.users")).unwrap()),
            vec!["log_users"]
        );
        assert_eq!(router.cache.len(), 2);
    }

    #[test]
    fn test_producer_props_passthrough_parsed_and_isolated() {
        let props: HashMap<String, String> = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("topic", "users"),
            // Producer overrides: prefix stripped, value kept verbatim.
            ("producer.linger.ms", "5"),
            ("producer.compression.codec", "lz4"),
            ("producer.queue.buffering.max.messages", "200000"),
            // The bare `linger.ms` is the INTERNAL batch linger, not a
            // librdkafka property — it must NOT leak into producer_props.
            ("linger.ms", "100"),
            ("batch.size", "500"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let cfg = KafkaSinkConfig::from_config(&ConnectorConfig::new(props));
        assert_eq!(
            cfg.producer_props,
            vec![
                ("compression.codec".to_string(), "lz4".to_string()),
                ("linger.ms".to_string(), "5".to_string()),
                (
                    "queue.buffering.max.messages".to_string(),
                    "200000".to_string()
                ),
            ],
            "producer.* options pass through with the prefix stripped (sorted)"
        );
        // The internal batch linger stays separate from the passthrough.
        assert_eq!(cfg.batch_timeout_ms, 100);
        assert_eq!(cfg.batch_size, 500);
    }

    #[test]
    fn test_topic_router_template_renders_per_table() {
        let mut router = TopicRouter::new("cdc_${table}", &[]).unwrap();
        assert_eq!(
            (*router.resolve_all(Some("shop.orders")).unwrap()),
            vec!["cdc_orders"],
            "identifier splits at the LAST dot"
        );
        assert_eq!(
            (*router.resolve_all(Some("shop.a.b")).unwrap()),
            vec!["cdc_b"],
            "dots inside table names keep everything after the last dot"
        );
        assert!(
            router.resolve_all(None).is_err(),
            "a template without origin identity must fail loudly"
        );
    }

    #[test]
    fn test_topic_router_all_matches_fan_out() {
        let routes: Vec<TopicRoute> = serde_json::from_str(
            r#"[
                {"pattern": "shop\\.orders_.*", "topic": "topic_orders"},
                {"pattern": "shop\\..*", "topic": "topic_shop_${table}"}
            ]"#,
        )
        .unwrap();
        let mut router = TopicRouter::new("cdc_catch_all", &routes).unwrap();
        // Overlapping routes deliver to EVERY match, in declaration order
        // (the Java table_topic_mappings fan-out semantics).
        assert_eq!(
            (*router.resolve_all(Some("shop.orders_2024")).unwrap()),
            vec!["topic_orders", "topic_shop_orders_2024"],
            "every matching route receives a copy"
        );
        assert_eq!(
            (*router.resolve_all(Some("shop.users")).unwrap()),
            vec!["topic_shop_users"],
            "a single match stays single"
        );
        assert_eq!(
            (*router.resolve_all(Some("other.db.users")).unwrap()),
            vec!["cdc_catch_all"],
            "unmatched rows fall back to the default topic"
        );
        // Patterns are anchored: a substring alone does not match.
        assert_eq!(
            (*router.resolve_all(Some("shopx.users")).unwrap()),
            vec!["cdc_catch_all"]
        );
    }

    #[test]
    fn test_topic_router_dedupes_identical_rendered_topics() {
        // Three routes match `shop.users`, two of them rendering the SAME
        // topic name — the duplicate collapses to one delivery.
        let routes: Vec<TopicRoute> = serde_json::from_str(
            r#"[
                {"pattern": "shop\\.users", "topic": "topic_users"},
                {"pattern": "shop\\..*", "topic": "topic_users"},
                {"pattern": "shop\\..*", "topic": "log_${table}"}
            ]"#,
        )
        .unwrap();
        let mut router = TopicRouter::new("t", &routes).unwrap();
        assert_eq!(
            (*router.resolve_all(Some("shop.users")).unwrap()),
            vec!["topic_users", "log_users"],
            "routes rendering the same topic deliver only once"
        );
    }

    #[test]
    fn test_topic_router_invalid_regex_fails_fast() {
        let routes: Vec<TopicRoute> =
            serde_json::from_str(r#"[{"pattern": "shop\\.(users", "topic": "t"}]"#).unwrap();
        assert!(TopicRouter::new("t", &routes).is_err());
    }

    #[test]
    fn test_topic_routes_config_parsing() {
        let props: HashMap<String, String> = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("topic", "cdc_${table}"),
            (
                "topic-routes",
                "[{\"pattern\": \"shop\\\\.orders.*\", \"topic\": \"topic_orders\"}]",
            ),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let config = KafkaSinkConfig::from_config(&ConnectorConfig::new(props));
        assert_eq!(config.topic_routes.len(), 1);
        assert_eq!(config.topic_routes[0].pattern, "shop\\.orders.*");
        assert_eq!(config.topic_routes[0].topic, "topic_orders");

        // Underscore alias parses too.
        let props: HashMap<String, String> =
            [("topic_routes", "[{\"pattern\": \"a\", \"topic\": \"b\"}]")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        assert_eq!(
            KafkaSinkConfig::from_config(&ConnectorConfig::new(props))
                .topic_routes
                .len(),
            1
        );

        // Invalid JSON degrades to no routes (with a warning), not a panic.
        let props: HashMap<String, String> = [("topic-routes", "not-json".to_string())]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(
            KafkaSinkConfig::from_config(&ConnectorConfig::new(props))
                .topic_routes
                .is_empty()
        );
    }

    #[test]
    fn test_canal_topic_routing_resolves_per_message_table() {
        // The canal path routes each ENCODED MESSAGE (not each row) so
        // paired updates and expired pairing deletes land on their own
        // table's topic(s).
        let routes: Vec<TopicRoute> = serde_json::from_str(
            r#"[
                {"pattern": "neworiental_v3\\.entity_question", "topic": "resource_binlog"},
                {"pattern": "neworiental_v3\\.entity_.*", "topic": "entity_binlog"}
            ]"#,
        )
        .unwrap();
        let props: HashMap<String, String> = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("topic", "cdc_${table}"),
            ("format", "canal_client_json"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let mut cfg = KafkaSinkConfig::from_config(&ConnectorConfig::new(props));
        cfg.topic_routes = routes;
        let mut writer = KafkaSinkWriter::new(cfg, metrics()).unwrap();
        for identifier in ["neworiental_v3.entity_question", "shop.orders"] {
            let schema = seatunnel_api::TableSchema::new(
                identifier,
                vec![
                    seatunnel_api::ColumnDef::new("id", seatunnel_api::ColumnType::Int64)
                        .primary_key(),
                    seatunnel_api::ColumnDef::new("name", seatunnel_api::ColumnType::String),
                ],
            );
            let event = seatunnel_api::SchemaChangeEvent::initial_schema(schema);
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(writer.apply_schema_change(&event))
                .unwrap();
        }

        let tagged_row = |kind: RowKind, id: i64, name: &str| {
            let mut row = Row::new(kind, 2);
            row.set(0, seatunnel_api::Field::Int64(id));
            row.set(1, seatunnel_api::Field::String(name.into()));
            row.origin_table = Some("neworiental_v3.entity_question".to_string());
            row
        };

        // An UPDATE (delete+insert pair) becomes ONE message that fans
        // out to BOTH matching topics — the real-business double-routing
        // case (entity_question → resource_binlog + question_html…).
        let mut encoder = writer.canal_encoder.take().unwrap();
        encoder
            .encode(&tagged_row(RowKind::Delete, 1, "old"))
            .unwrap();
        let messages = encoder
            .encode(&tagged_row(RowKind::Insert, 1, "new"))
            .unwrap();
        writer.canal_encoder = Some(encoder);
        assert_eq!(messages.len(), 1, "paired update is one message");
        assert_eq!(
            writer
                .topic_router
                .resolve_all(Some(messages[0].table.as_str()))
                .unwrap()
                .as_ref()
                .clone(),
            vec!["resource_binlog", "entity_binlog"],
            "both matching routes receive the same update message"
        );

        // A single-table row resolves through the template fallback.
        let mut encoder = writer.canal_encoder.take().unwrap();
        let mut row = Row::new(RowKind::Insert, 2);
        row.set(0, seatunnel_api::Field::Int64(1));
        row.set(1, seatunnel_api::Field::String("a".into()));
        row.origin_table = Some("shop.orders".to_string());
        let messages = encoder.encode(&row).unwrap();
        writer.canal_encoder = Some(encoder);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].table, "shop.orders");
        assert_eq!(
            writer
                .topic_router
                .resolve_all(Some(messages[0].table.as_str()))
                .unwrap()
                .as_ref()
                .clone(),
            vec!["cdc_orders"]
        );
    }

    #[tokio::test]
    async fn test_in_flight_tracker_fifo_reap_and_drain() {
        let mut tracker = InFlightDeliveries::new();
        let metrics = seatunnel_api::sink::SinkMetrics::new();
        // Push in order: ok, err, ok.
        tracker.push("t1", "k1", "p1", async { Ok(()) });
        tracker.push("t2", "k2", "p2", async { Err("boom t2".to_string()) });
        tracker.push("t3", "k3", "p3", async { Ok(()) });
        assert_eq!(tracker.len(), 3);

        // Partial reap (FIFO order) surfaces the failure.
        let (delivered, failed, errors) = tracker.reap(2, &metrics).await;
        assert_eq!((delivered, failed), (1, 1));
        assert_eq!(
            errors,
            vec!["boom t2 (topic=t2, key=k2, after 0ms, payload~'p2')"]
        );
        assert_eq!(tracker.len(), 1);

        // Drain finishes the rest.
        let delivered = tracker.drain(&metrics).await.unwrap();
        assert_eq!(delivered, 1);
        assert_eq!(tracker.len(), 0);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sent, 0, "push itself does not count as sent");
        assert_eq!(snapshot.delivered, 2);
        assert_eq!(snapshot.failed, 1);
        assert!(snapshot.last_error.as_deref().unwrap().contains("boom t2"));
    }

    #[tokio::test]
    async fn test_in_flight_drain_aggregates_error_details() {
        let mut tracker = InFlightDeliveries::new();
        let metrics = seatunnel_api::sink::SinkMetrics::new();
        for i in 0..5 {
            tracker.push(
                &format!("topic-{i}"),
                &format!("key-{i}"),
                &format!("payload-{i}"),
                async move { Err(format!("err-{i}")) },
            );
        }
        let err = tracker.drain(&metrics).await.unwrap_err().to_string();
        assert!(err.contains("5/5"), "message names the total: {err}");
        // Only the first few details are embedded.
        assert!(err.contains("err-0") && err.contains("err-2"));
        assert!(!err.contains("err-4"), "details capped at 3: {err}");
    }

    #[tokio::test]
    async fn test_in_flight_reap_to_limit_is_noop_below_limit() {
        let mut tracker = InFlightDeliveries::new();
        let metrics = seatunnel_api::sink::SinkMetrics::new();
        tracker.push("t", "k", "p", async { Ok(()) });
        // Under the limit: returns without awaiting (verified by the
        // queue staying full afterwards until drain).
        tracker.reap_to_limit(&metrics).await.unwrap();
        assert_eq!(tracker.len(), 1);
        tracker.drain(&metrics).await.unwrap();
    }
}
