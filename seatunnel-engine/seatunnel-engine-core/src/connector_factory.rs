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

//! Connector factory: builds engine-ready Source readers, Transform chains and
//! Sink writers from job configuration.
//!
//! This is the single place where plugin names map to concrete connector
//! implementations. Both the cluster WorkerNode and the CLI local runner use
//! it, guaranteeing identical behavior in both modes.
//!
//! The factory returns type-erased boxes (`BoxedSourceReader` /
//! `BoxedSinkWriter`) so the execution layer never needs to know which
//! connector is running.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use seatunnel_api::row::{Field, Row};
use seatunnel_api::schema::TableSchema;
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::transform::Transform;
use serde::Serialize;

/// A split handle whose concrete payload is erased by the reader adapter.
/// The engine's chained TaskGroup lets readers self-manage splits, so the
/// engine only needs an opaque identifier.
#[derive(Debug, Clone)]
pub struct AnySplit {
    id: String,
}

impl AnySplit {
    pub fn new(id: impl Into<String>) -> Self {
        AnySplit { id: id.into() }
    }
}

impl SourceSplit for AnySplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Type-erased source reader producing plain `Row`s.
pub type BoxedSourceReader = Box<dyn SourceReader<Output = Row, Split = AnySplit>>;

/// Type-erased sink writer consuming `Row`s with byte-serialized states.
pub type BoxedSinkWriter =
    Box<dyn SinkWriter<Input = Row, WriterState = Vec<u8>, CommitInfo = Vec<u8>>>;

/// Type-erased 2PC committer operating on JSON-serialized commit infos.
pub type BoxedSinkCommitter = Box<
    dyn seatunnel_api::sink::sink_committer::SinkCommitter<
            CommitInfo = Vec<u8>,
            AggregatedCommitInfo = serde_json::Value,
        >,
>;

/// Adapts a typed [`SinkCommitter`] to the byte-serialized engine surface,
/// mirroring what [`SinkWriterAdapter`] does for writers.
pub(crate) struct CommitterAdapter<C> {
    pub(crate) inner: C,
}

impl<C> seatunnel_api::sink::sink_committer::SinkCommitter for CommitterAdapter<C>
where
    C: seatunnel_api::sink::sink_committer::SinkCommitter + Send,
    C::CommitInfo: serde::de::DeserializeOwned + Serialize + Send + Sync,
    C::AggregatedCommitInfo: Serialize + Send + Sync,
{
    type CommitInfo = Vec<u8>;
    type AggregatedCommitInfo = serde_json::Value;

    fn commit(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> seatunnel_api::sink::sink_committer::CommitterFuture<'_, Self::AggregatedCommitInfo> {
        Box::pin(async move {
            let mut typed = Vec::with_capacity(commit_infos.len());
            for bytes in commit_infos {
                typed.push(
                    serde_json::from_slice(&bytes)
                        .map_err(|e| anyhow::anyhow!("deserialize commit info: {}", e))?,
                );
            }
            let aggregated = self.inner.commit(typed).await?;
            serde_json::to_value(&aggregated)
                .map_err(|e| anyhow::anyhow!("serialize aggregated commit info: {}", e))
        })
    }

    fn abort(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> seatunnel_api::sink::sink_committer::CommitterFuture<'_, ()> {
        Box::pin(async move {
            let mut typed = Vec::with_capacity(commit_infos.len());
            for bytes in commit_infos {
                typed.push(
                    serde_json::from_slice(&bytes)
                        .map_err(|e| anyhow::anyhow!("deserialize commit info: {}", e))?,
                );
            }
            self.inner.abort(typed).await
        })
    }
}

/// One pipeline's sink side: the (possibly multiplexed) writer plus the
/// optional 2PC committer driven by the engine at checkpoint completion.
pub struct SinkPipeline {
    pub writer: BoxedSinkWriter,
    pub committer: Option<BoxedSinkCommitter>,
}

/// Type-erased transform chain element.
pub type BoxedTransform = Box<dyn Transform<Input = Row, Output = Row>>;

// ---------------------------------------------------------------------------
// Reader adapter
// ---------------------------------------------------------------------------

struct ReaderAdapter<R> {
    inner: R,
    warned_splits: bool,
}

impl<R> SourceReader for ReaderAdapter<R>
where
    R: SourceReader + Send,
    R::Output: Into<Row>,
{
    type Output = Row;
    type Split = AnySplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.open().await })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            match self.inner.poll_next().await? {
                PollResult::Record(out) => Ok(PollResult::Record(out.into())),
                PollResult::SchemaChange(event) => Ok(PollResult::SchemaChange(event)),
                PollResult::Empty => Ok(PollResult::Empty),
                PollResult::EOF => Ok(PollResult::EOF),
            }
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        Box::pin(async move { self.inner.snapshot_state().await })
    }

    fn notify_checkpoint_complete(
        &mut self,
        checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.notify_checkpoint_complete(checkpoint_id).await })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {
        // Readers managed by this engine enumerate their own splits; external
        // split assignment is not part of the chained execution model yet.
        if !self.warned_splits {
            self.warned_splits = true;
            tracing::debug!("ReaderAdapter: external add_splits ignored (readers self-enumerate)");
        }
    }

    fn handle_no_more_splits(&mut self) {
        self.inner.handle_no_more_splits();
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.close().await })
    }
}

// ---------------------------------------------------------------------------
// Sink adapter
// ---------------------------------------------------------------------------

pub(crate) struct SinkWriterAdapter<W> {
    pub(crate) inner: W,
}

#[async_trait::async_trait]
impl<W> SinkWriter for SinkWriterAdapter<W>
where
    W: SinkWriter<Input = Row> + Send,
    W::CommitInfo: Serialize + Send,
    W::WriterState: Serialize + Send,
{
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = Vec<u8>;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.open().await })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.write(record).await })
    }

    fn prepare_commit(
        &mut self,
        checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            let infos = self.inner.prepare_commit(checkpoint_id).await?;
            Ok(infos
                .iter()
                .map(|info| serde_json::to_vec(info).unwrap_or_default())
                .collect())
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        // The inner writer already returns serialized bytes; forwarding them
        // directly keeps the payload single-encoded for restore.
        Box::pin(async move { self.inner.snapshot_state().await })
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.close().await })
    }

    fn apply_schema_change(
        &mut self,
        event: &seatunnel_api::SchemaChangeEvent,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let event = event.clone();
        Box::pin(async move { self.inner.apply_schema_change(&event).await })
    }

    fn poll_flush(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.poll_flush().await })
    }
}

// ---------------------------------------------------------------------------
// Built-in fake source (smoke tests / local demos)
// ---------------------------------------------------------------------------

/// Emits synthetic rows. `total` bounds the stream; a `total` of `u64::MAX`
/// (config `row.num < 0`, e.g. `-1`) never ends, which together with
/// `sleep.ms` models a continuous streaming source for demos and metrics.
#[derive(Debug, Default)]
pub struct FakeSeqSource {
    emitted: u64,
    total: u64,
    /// Optional inter-row delay for unbounded demos (0 = as fast as
    /// possible).
    sleep_ms: u64,
}

impl FakeSeqSource {
    pub fn with_total(total: u64) -> Self {
        FakeSeqSource {
            emitted: 0,
            total,
            sleep_ms: 0,
        }
    }

    pub fn with_sleep_ms(mut self, sleep_ms: u64) -> Self {
        self.sleep_ms = sleep_ms;
        self
    }
}

impl SourceReader for FakeSeqSource {
    type Output = Row;
    type Split = AnySplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if self.emitted >= self.total {
                return Ok(PollResult::EOF);
            }
            if self.sleep_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(self.sleep_ms)).await;
            }
            let mut row = Row::new(seatunnel_api::row::RowKind::Insert, 3);
            row.set(0, Field::Int64(self.emitted as i64));
            row.set(1, Field::String(format!("fake-row-{}", self.emitted)));
            row.set(2, Field::Bool(self.emitted.is_multiple_of(2)));
            self.emitted += 1;
            Ok(PollResult::Record(row))
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        Box::pin(async move { Ok(serde_json::to_vec(&self.emitted).unwrap()) })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// Built-in console sink (used by local runs and smoke tests)
// ---------------------------------------------------------------------------

/// Sink writer that prints every row as a JSON object on stdout.
pub struct ConsoleSinkWriter {
    prefix: String,
    written: u64,
}

impl ConsoleSinkWriter {
    pub fn new(prefix: impl Into<String>) -> Self {
        ConsoleSinkWriter {
            prefix: prefix.into(),
            written: 0,
        }
    }
}

#[async_trait::async_trait]
impl SinkWriter for ConsoleSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = Vec<u8>;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        self.written += 1;
        let line = row_to_json(&record);
        Box::pin(async move {
            println!("{}{}", self.prefix, line);
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
        _checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let n = self.written;
        Box::pin(
            async move { Ok(serde_json::to_vec(&serde_json::json!({ "written": n })).unwrap()) },
        )
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

fn field_to_json(field: &Field) -> serde_json::Value {
    use serde_json::Value;
    match field {
        Field::Null => Value::Null,
        Field::Bool(b) => Value::Bool(*b),
        Field::Int8(v) => (*v).into(),
        Field::Int16(v) => (*v).into(),
        Field::Int32(v) => (*v).into(),
        Field::Int64(v) => (*v).into(),
        Field::UInt8(v) => (*v).into(),
        Field::UInt16(v) => (*v).into(),
        Field::UInt32(v) => (*v).into(),
        Field::UInt64(v) => (*v).into(),
        Field::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Float64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::String(s) => Value::String(s.clone()),
        Field::Bytes(b) => Value::String(format!("0x{}", hex::encode(b))),
        Field::Decimal(d) => Value::String(d.to_string()),
        Field::Json(s) => s.clone(),
        Field::Date(d) => Value::String(d.to_string()),
        Field::Time(t) => Value::String(t.to_string()),
        Field::DateTime(dt) => Value::String(dt.to_string()),
        Field::TimestampTz(ts) => Value::String(ts.to_string()),
        Field::Duration(d) => (*d).into(),
        Field::Array(items) => Value::Array(items.iter().map(field_to_json).collect()),
        Field::Row(inner) => {
            let obj: serde_json::Map<String, serde_json::Value> = inner
                .iter()
                .enumerate()
                .map(|(i, f)| (format!("f{}", i), field_to_json(f)))
                .collect();
            Value::Object(obj)
        }
    }
}

fn row_to_json(row: &Row) -> String {
    let obj: serde_json::Map<String, serde_json::Value> = (0..row.field_count())
        .map(|i| (format!("f{}", i), field_to_json(row.get(i))))
        .collect();
    serde_json::Value::Object(obj).to_string()
}

// ---------------------------------------------------------------------------
// Built-in filter transform (index-based comparisons)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    NotNull,
    IsNull,
}

impl CompareOp {
    fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "=" | "eq" | "equals" => Ok(CompareOp::Eq),
            "!=" | "ne" | "not_equals" => Ok(CompareOp::Ne),
            ">" | "gt" => Ok(CompareOp::Gt),
            "<" | "lt" => Ok(CompareOp::Lt),
            ">=" | "gte" | "gteq" => Ok(CompareOp::Gte),
            "<=" | "lte" | "lteq" => Ok(CompareOp::Lte),
            "not_null" | "notnull" => Ok(CompareOp::NotNull),
            "is_null" | "isnull" | "null" => Ok(CompareOp::IsNull),
            other => Err(anyhow::anyhow!("unsupported filter operator '{}'", other)),
        }
    }
}

/// Filter rows by comparing the numeric/string value of one field index.
pub struct FilterByIndexTransform {
    index: usize,
    op: CompareOp,
    value: Option<Field>,
}

impl FilterByIndexTransform {
    // Private: callers go through `create_transforms`, which parses the
    // crate-private `CompareOp`.
    fn new(index: usize, op: CompareOp, value: Option<Field>) -> Self {
        FilterByIndexTransform { index, op, value }
    }
}

fn numeric_value(field: &Field) -> Option<f64> {
    match field {
        Field::Int8(v) => Some(*v as f64),
        Field::Int16(v) => Some(*v as f64),
        Field::Int32(v) => Some(*v as f64),
        Field::Int64(v) => Some(*v as f64),
        Field::UInt8(v) => Some(*v as f64),
        Field::UInt16(v) => Some(*v as f64),
        Field::UInt32(v) => Some(*v as f64),
        Field::UInt64(v) => Some(*v as f64),
        Field::Float32(v) => Some(*v as f64),
        Field::Float64(v) => Some(*v),
        Field::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

impl Transform for FilterByIndexTransform {
    type Input = Row;
    type Output = Row;

    fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
        if self.index >= record.field_count() {
            return Ok(vec![]);
        }
        let field = record.get(self.index);
        let keep = match self.op {
            CompareOp::NotNull => !matches!(field, Field::Null),
            CompareOp::IsNull => matches!(field, Field::Null),
            CompareOp::Eq | CompareOp::Ne => {
                let eq = match (&self.value, field) {
                    (Some(Field::String(want)), Field::String(got)) => want == got,
                    (Some(want), got) => match (numeric_value(want), numeric_value(got)) {
                        (Some(a), Some(b)) => (a - b).abs() < f64::EPSILON,
                        _ => false,
                    },
                    _ => false,
                };
                if self.op == CompareOp::Eq { eq } else { !eq }
            }
            CompareOp::Gt | CompareOp::Lt | CompareOp::Gte | CompareOp::Lte => {
                let (Some(a), Some(b)) = (
                    self.value.as_ref().and_then(numeric_value),
                    numeric_value(field),
                ) else {
                    return Ok(vec![]);
                };
                match self.op {
                    CompareOp::Gt => b > a,
                    CompareOp::Lt => b < a,
                    CompareOp::Gte => b >= a,
                    CompareOp::Lte => b <= a,
                    _ => unreachable!(),
                }
            }
        };
        Ok(if keep { vec![record] } else { vec![] })
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        None
    }

    fn set_input_schema(&mut self, _schema: TableSchema) {}
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

/// Convert a JSON config object into the flat `HashMap<String,String>` shape
/// that `ConnectorConfig` consumes. Nested objects are serialized back to
/// their dotted-path representation so `startup.mode`, `database-name` etc.
/// survive round-trips through the gRPC descriptor.
pub fn json_to_config_map(value: &serde_json::Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    fn walk(prefix: &str, v: &serde_json::Value, out: &mut HashMap<String, String>) {
        match v {
            serde_json::Value::Object(map) => {
                for (k, child) in map {
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{}.{}", prefix, k)
                    };
                    walk(&key, child, out);
                }
            }
            serde_json::Value::Array(items) => {
                // Arrays are stored as comma-separated scalars when they hold
                // scalars, otherwise as JSON text.
                if items
                    .iter()
                    .all(|i| i.is_string() || i.is_number() || i.is_boolean())
                {
                    let joined = items
                        .iter()
                        .map(scalar_to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    out.insert(prefix.to_string(), joined);
                } else if let Ok(text) = serde_json::to_string(v) {
                    out.insert(prefix.to_string(), text);
                }
            }
            scalar => {
                out.insert(prefix.to_string(), scalar_to_string(scalar));
            }
        }
    }
    walk("", value, &mut out);
    out
}

fn scalar_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Factory entry points
// ---------------------------------------------------------------------------

/// Build the source reader described by `plugin` + flat config.
///
/// `restore_state` carries the serialized checkpoint state captured at the
/// last completed checkpoint; connectors that support it (CDC readers)
/// resume from that position instead of starting over.
///
/// Supported plugins: MySQL-CDC (`MySqlCdc`), Postgres-CDC, TiDB-CDC, Kafka,
/// JDBC and the built-in FakeSource. Plugin matching is case-insensitive and
/// tolerates common aliases so YAML configs read naturally.
pub fn create_source(
    plugin: &str,
    config: &HashMap<String, String>,
    parallelism: usize,
    restore_state: Option<&[u8]>,
) -> anyhow::Result<BoxedSourceReader> {
    use seatunnel_connector_common::ConnectorConfig;
    let conn = ConnectorConfig::new(config.clone());
    let lower = plugin.to_lowercase().replace(['-', '_'], "");

    match lower.as_str() {
        "mysqlcdc" | "mysql" | "mysqlcdcsource" | "cdcmysql" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_cdc_mysql::{MySqlCdcConfig, MySqlCdcReader};
                let cfg = MySqlCdcConfig::from_config(&conn);
                tracing::info!(
                    "factory: MySQL-CDC source → {}:{} db={} table={} startup={:?}",
                    cfg.hostname,
                    cfg.port,
                    cfg.database_name,
                    cfg.table_name,
                    cfg.startup_mode
                );
                let mut reader = MySqlCdcReader::new(cfg, None);
                if let Some(bytes) = restore_state {
                    reader
                        .restore_from_state_bytes(bytes)
                        .map_err(|e| anyhow::anyhow!("restore MySQL CDC state: {}", e))?;
                }
                Ok(Box::new(ReaderAdapter {
                    inner: reader,
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!(
                    "MySQL CDC connector not compiled in (feature connector-mysql)"
                ))
            }
        }
        "postgrescdc" | "postgres" | "postgresqlcdc" | "cdcpostgres" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_cdc_postgres::{PostgresCdcConfig, PostgresCdcReader};
                let cfg = PostgresCdcConfig::from_config(&conn);
                tracing::info!(
                    "factory: Postgres-CDC source → {}:{} db={} table={}.{}",
                    cfg.hostname,
                    cfg.port,
                    cfg.database_name,
                    cfg.schema_name,
                    cfg.table_name
                );
                let mut reader = PostgresCdcReader::new(cfg, None);
                if let Some(bytes) = restore_state {
                    reader
                        .restore_from_state_bytes(bytes)
                        .map_err(|e| anyhow::anyhow!("restore Postgres CDC state: {}", e))?;
                }
                Ok(Box::new(ReaderAdapter {
                    inner: reader,
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("Postgres CDC connector not compiled in"))
            }
        }
        "tidbcdc" | "tidb" | "cdctidb" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_cdc_tidb::{TiDBCdcConfig, TiDBCdcReader};
                let cfg = TiDBCdcConfig::from_config(&conn);
                tracing::info!(
                    "factory: TiDB-CDC source → pd={:?} db={} table={} sql={}:{}",
                    cfg.pd_addrs,
                    cfg.database_name,
                    cfg.table_name,
                    cfg.conn.host,
                    cfg.conn.port
                );
                let mut reader = TiDBCdcReader::new(cfg, None);
                if let Some(bytes) = restore_state {
                    reader
                        .restore_from_state_bytes(bytes)
                        .map_err(|e| anyhow::anyhow!("restore TiDB CDC state: {}", e))?;
                }
                Ok(Box::new(ReaderAdapter {
                    inner: reader,
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("TiDB CDC connector not compiled in"))
            }
        }
        "kafka" | "kafkasource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_kafka::{KafkaSourceConfig, KafkaSourceReader};
                let cfg = KafkaSourceConfig::from_config(&conn);
                tracing::info!(
                    "factory: Kafka source topic={} brokers={}",
                    cfg.topic,
                    cfg.bootstrap_servers
                );
                let mut reader = KafkaSourceReader::new(cfg, None);
                if let Some(bytes) = restore_state {
                    reader
                        .restore_from_state_bytes(bytes)
                        .map_err(|e| anyhow::anyhow!("restore Kafka source state: {}", e))?;
                }
                Ok(Box::new(ReaderAdapter {
                    inner: reader,
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("Kafka connector not compiled in"))
            }
        }
        "jdbc" | "jdbcsource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_jdbc::{JdbcSourceConfig, JdbcSourceReader};
                let cfg = JdbcSourceConfig::from_config(&conn);
                tracing::info!(
                    "factory: JDBC source url={}",
                    cfg.url.replace(&cfg.password, "***")
                );
                let mut reader = JdbcSourceReader::new(cfg, None);
                if let Some(bytes) = restore_state {
                    reader
                        .restore_from_state_bytes(bytes)
                        .map_err(|e| anyhow::anyhow!("restore JDBC source state: {}", e))?;
                }
                Ok(Box::new(ReaderAdapter {
                    inner: reader,
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("JDBC connector not compiled in"))
            }
        }
        "redis" | "redissource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_redis::{RedisConfig, RedisSourceReader};
                let cfg = RedisConfig::from_config(&conn);
                tracing::info!(
                    "factory: Redis source {}:{} pattern={}",
                    cfg.host,
                    cfg.port,
                    cfg.keys_pattern
                );
                let _ = restore_state;
                Ok(Box::new(ReaderAdapter {
                    inner: RedisSourceReader::new(cfg),
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("Redis connector not compiled in"))
            }
        }
        "elasticsearch" | "es" | "essource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_elasticsearch::{EsConfig, EsSourceReader};
                let cfg = EsConfig::from_config(&conn);
                tracing::info!(
                    "factory: Elasticsearch source index={} hosts={:?}",
                    cfg.index,
                    cfg.hosts
                );
                let _ = restore_state;
                Ok(Box::new(ReaderAdapter {
                    inner: EsSourceReader::new(cfg),
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("Elasticsearch connector not compiled in"))
            }
        }
        "rabbitmq" | "rabbitmqsource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_rabbitmq::{RabbitMqConfig, RabbitMqSourceReader};
                let cfg = RabbitMqConfig::from_config(&conn);
                tracing::info!(
                    "factory: RabbitMQ source queue={} at {}:{} vhost={}",
                    cfg.queue_name,
                    cfg.host,
                    cfg.port,
                    cfg.virtual_host
                );
                // Position restore is implicit: unacked deliveries are
                // redelivered by the broker after a restart.
                let _ = restore_state;
                Ok(Box::new(ReaderAdapter {
                    inner: RabbitMqSourceReader::new(cfg),
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("RabbitMQ connector not compiled in"))
            }
        }
        "http" | "httpsource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_http::{HttpSourceConfig, HttpSourceReader};
                let cfg = HttpSourceConfig::from_config(&conn);
                tracing::info!(
                    "factory: HTTP source {} {} (poll={}ms)",
                    cfg.method,
                    cfg.url,
                    cfg.poll_interval_ms
                );
                let _ = restore_state;
                Ok(Box::new(ReaderAdapter {
                    inner: HttpSourceReader::new(cfg),
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("HTTP connector not compiled in"))
            }
        }
        "clickhouse" | "clickhousesource" | "chsource" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_clickhouse::{ClickHouseConfig, ClickHouseSourceReader};
                let cfg = ClickHouseConfig::from_config(&conn);
                tracing::info!(
                    "factory: ClickHouse source {} ({})",
                    cfg.qualified_table(),
                    cfg.url
                );
                let mut reader = ClickHouseSourceReader::new(cfg);
                if let Some(bytes) = restore_state {
                    reader
                        .restore_from_state_bytes(bytes)
                        .map_err(|e| anyhow::anyhow!("restore ClickHouse source state: {}", e))?;
                }
                Ok(Box::new(ReaderAdapter {
                    inner: reader,
                    warned_splits: false,
                }))
            }
            #[cfg(not(feature = "connectors"))]
            {
                let _ = (parallelism, restore_state);
                Err(anyhow::anyhow!("ClickHouse connector not compiled in"))
            }
        }
        "fake" | "fake source" | "fakesource" => {
            // `row.num < 0` (e.g. -1) = unbounded stream for demos;
            // `sleep.ms` adds an inter-row delay to model throughput.
            let _ = (parallelism, restore_state);
            let total = match config.get("row.num").and_then(|v| v.parse::<i64>().ok()) {
                Some(n) if n < 0 => u64::MAX,
                Some(n) => n as u64,
                None => 10,
            };
            let sleep_ms = config
                .get("sleep.ms")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            Ok(Box::new(ReaderAdapter {
                inner: FakeSeqSource::with_total(total).with_sleep_ms(sleep_ms),
                warned_splits: false,
            }))
        }
        other => Err(anyhow::anyhow!("unknown source plugin '{}'", other)),
    }
}

/// Build the sink writer described by `plugin` + flat config.
/// Build one sink writer. Legacy entry point (no restore, no committer).
pub fn create_sink(
    plugin: &str,
    config: &HashMap<String, String>,
) -> anyhow::Result<BoxedSinkWriter> {
    Ok(create_sink_with_restore(plugin, config, None)?.writer)
}

/// Build one sink writer plus its optional 2PC committer. `restore` is the
/// writer state captured at the checkpoint being restored from (serialized
/// `snapshot_state` payload, possibly the fan-out `{name: state}` map for
/// a specific sink); sinks without meaningful writer state ignore it.
pub fn create_sink_with_restore(
    plugin: &str,
    config: &HashMap<String, String>,
    restore: Option<&[u8]>,
) -> anyhow::Result<SinkPipeline> {
    use seatunnel_connector_common::ConnectorConfig;
    let conn = ConnectorConfig::new(config.clone());
    let lower = plugin.to_lowercase().replace(['-', '_'], "");

    match lower.as_str() {
        "kafka" | "kafkasink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_kafka::{
                    KafkaSinkCommitter, KafkaSinkConfig, KafkaSinkWriter,
                };
                let cfg = KafkaSinkConfig::from_config(&conn);
                tracing::info!(
                    "factory: Kafka sink topic={} brokers={} acks={} txn={}",
                    cfg.topic,
                    cfg.bootstrap_servers,
                    cfg.acks,
                    cfg.transactions_enabled
                );
                let mut writer = KafkaSinkWriter::new(cfg)?;
                if let Some(bytes) = restore {
                    writer.restore_from_state_bytes(bytes)?;
                }
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter { inner: writer }),
                    committer: Some(Box::new(CommitterAdapter {
                        inner: KafkaSinkCommitter::new(),
                    })),
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("Kafka connector not compiled in"))
            }
        }
        "jdbc" | "jdbcsink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_jdbc::{JdbcSinkConfig, JdbcSinkWriter};
                let cfg = JdbcSinkConfig::from_config(&conn);
                tracing::info!("factory: JDBC sink");
                if restore.is_some() {
                    tracing::debug!("factory: JDBC sink writer state restore ignored (stateless)");
                }
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter {
                        inner: JdbcSinkWriter::new(cfg, None),
                    }),
                    committer: None,
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("JDBC connector not compiled in"))
            }
        }
        "jdbcxa" | "jdbc-xa" | "xa" | "mysqlxa" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_jdbc::{XaSinkCommitter, XaSinkConfig, XaSinkWriter};
                let cfg = XaSinkConfig::from_config(&conn);
                tracing::info!(
                    "factory: JDBC XA sink table={} url={} xid-prefix={}",
                    cfg.table,
                    cfg.url,
                    cfg.xid_prefix
                );
                let mut writer = XaSinkWriter::new(cfg.clone());
                if let Some(bytes) = restore {
                    writer.restore_from_state_bytes(bytes)?;
                }
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter { inner: writer }),
                    committer: Some(Box::new(CommitterAdapter {
                        inner: XaSinkCommitter::new(
                            cfg.url.clone(),
                            cfg.username.clone(),
                            cfg.password.clone(),
                        ),
                    })),
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("JDBC XA connector not compiled in"))
            }
        }
        "redis" | "redissink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_redis::{RedisConfig, RedisSinkWriter};
                let cfg = RedisConfig::from_config(&conn);
                tracing::info!(
                    "factory: Redis sink {}:{} data-type={:?}",
                    cfg.host,
                    cfg.port,
                    cfg.data_type
                );
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter {
                        inner: RedisSinkWriter::new(cfg),
                    }),
                    committer: None,
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("Redis connector not compiled in"))
            }
        }
        "elasticsearch" | "es" | "essink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_elasticsearch::{EsConfig, EsSinkWriter};
                let cfg = EsConfig::from_config(&conn);
                tracing::info!(
                    "factory: Elasticsearch sink index={} hosts={:?}",
                    cfg.index,
                    cfg.hosts
                );
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter {
                        inner: EsSinkWriter::new(cfg, None),
                    }),
                    committer: None,
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("Elasticsearch connector not compiled in"))
            }
        }
        "rabbitmq" | "rabbitmqsink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_rabbitmq::{RabbitMqConfig, RabbitMqSinkWriter};
                let cfg = RabbitMqConfig::from_config(&conn);
                tracing::info!(
                    "factory: RabbitMQ sink exchange='{}' routing_key='{}' at {}:{} (confirm={})",
                    cfg.exchange,
                    if cfg.routing_key.is_empty() {
                        &cfg.queue_name
                    } else {
                        &cfg.routing_key
                    },
                    cfg.host,
                    cfg.port,
                    cfg.publisher_confirm
                );
                let mut writer = RabbitMqSinkWriter::new(cfg);
                if let Some(bytes) = restore {
                    let _ = writer.restore_from_state_bytes(bytes);
                }
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter { inner: writer }),
                    committer: None,
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("RabbitMQ connector not compiled in"))
            }
        }
        "http" | "httpsink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_http::{HttpSinkConfig, HttpSinkWriter};
                let cfg = HttpSinkConfig::from_config(&conn);
                tracing::info!(
                    "factory: HTTP sink {} {} (batch-size={})",
                    cfg.method,
                    cfg.url,
                    cfg.batch_size
                );
                let mut writer = HttpSinkWriter::new(cfg, None);
                if let Some(bytes) = restore {
                    let _ = writer.restore_from_state_bytes(bytes);
                }
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter { inner: writer }),
                    committer: None,
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("HTTP connector not compiled in"))
            }
        }
        "clickhouse" | "clickhousesink" | "chsink" => {
            #[cfg(feature = "connectors")]
            {
                use seatunnel_connector_clickhouse::{ClickHouseConfig, ClickHouseSinkWriter};
                let cfg = ClickHouseConfig::from_config(&conn);
                tracing::info!(
                    "factory: ClickHouse sink {} ({})",
                    cfg.qualified_table(),
                    cfg.url
                );
                let mut writer = ClickHouseSinkWriter::new(cfg, None);
                if let Some(bytes) = restore {
                    let _ = writer.restore_from_state_bytes(bytes);
                }
                Ok(SinkPipeline {
                    writer: Box::new(SinkWriterAdapter { inner: writer }),
                    committer: None,
                })
            }
            #[cfg(not(feature = "connectors"))]
            {
                Err(anyhow::anyhow!("ClickHouse connector not compiled in"))
            }
        }
        "console" | "consolesink" | "" => Ok(SinkPipeline {
            writer: Box::new(SinkWriterAdapter {
                inner: ConsoleSinkWriter::new("[console] "),
            }),
            committer: None,
        }),
        other => Err(anyhow::anyhow!("unknown sink plugin '{}'", other)),
    }
}

/// One sink declaration of a pipeline: plugin name + its config object.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SinkDeclaration {
    pub plugin: String,
    pub config: serde_json::Value,
}

/// Parse a pipeline's `sinks` section into declarations. Accepts:
/// - array of single-key blocks: `[{Kafka: {...}}, {JDBC: {...}}]`
/// - multi-key map: `{Kafka: {...}, JDBC: {...}}` (order not guaranteed)
/// - a single block map (legacy `sink:` shape)
pub fn parse_sink_declarations(
    section: &serde_json::Value,
) -> anyhow::Result<Vec<SinkDeclaration>> {
    let blocks: Vec<serde_json::Value> = match section {
        serde_json::Value::Array(items) => items.clone(),
        serde_json::Value::Object(_) => vec![section.clone()],
        serde_json::Value::Null => Vec::new(),
        other => anyhow::bail!("sink section must be a map or list, got: {}", other),
    };
    let mut sinks = Vec::new();
    for block in blocks {
        let Some(map) = block.as_object() else {
            anyhow::bail!("each sink entry must be a map of {{PluginName: {{...}}}}");
        };
        for (plugin, config) in map {
            sinks.push(SinkDeclaration {
                plugin: plugin.clone(),
                config: config.clone(),
            });
        }
    }
    if sinks.is_empty() {
        anyhow::bail!("pipeline has no sinks configured");
    }
    Ok(sinks)
}

/// Build the sink writer(s) for a pipeline. A single sink returns the
/// writer directly; multiple sinks return a [`FanoutSinkWriter`] mux so
/// one reader broadcasts to all of them concurrently.
pub fn create_sinks(
    sinks: &[SinkDeclaration],
    failure_policy: crate::fanout::SinkFailurePolicy,
) -> anyhow::Result<BoxedSinkWriter> {
    Ok(create_sink_pipeline(sinks, failure_policy, None)?.writer)
}

/// Build a pipeline's full sink side (writer + optional 2PC committer).
///
/// `restore_writer_state` is the pipeline's writer state captured at the
/// checkpoint being restored from: for a single sink the sink's own
/// payload, for fan-out the merged `{sink-name: state}` JSON map which is
/// split back per sink here.
pub fn create_sink_pipeline(
    sinks: &[SinkDeclaration],
    failure_policy: crate::fanout::SinkFailurePolicy,
    restore_writer_state: Option<&[u8]>,
) -> anyhow::Result<SinkPipeline> {
    if sinks.len() == 1 {
        let sink = &sinks[0];
        let config = json_to_config_map(&sink.config);
        return create_sink_with_restore(&sink.plugin, &config, restore_writer_state);
    }
    let merged: HashMap<String, Vec<u8>> = restore_writer_state
        .and_then(|bytes| serde_json::from_slice(bytes).ok())
        .unwrap_or_default();
    let mut writers = Vec::new();
    let mut committers = Vec::new();
    for (idx, sink) in sinks.iter().enumerate() {
        let config = json_to_config_map(&sink.config);
        let name = format!("{}#{}", sink.plugin, idx);
        let state = merged.get(&name).map(|v| v.as_slice());
        let pipeline = create_sink_with_restore(&sink.plugin, &config, state)?;
        writers.push((name.clone(), pipeline.writer));
        committers.push((name, pipeline.committer));
    }
    let mux: BoxedSinkWriter = Box::new(crate::fanout::FanoutSinkWriter::new(
        writers,
        failure_policy,
    ));
    let committer = crate::fanout::FanoutCommitter::new(committers)
        .map(|committer| Box::new(committer) as BoxedSinkCommitter);
    Ok(SinkPipeline {
        writer: mux,
        committer,
    })
}

/// Build a transform chain from the ordered list of transform configs.
///
/// Recognized transform types:
/// - `filter` with `{field_index, operator, value}`
///   Unknown transforms produce an error — failing fast beats silently dropping
///   user logic.
pub fn create_transforms(configs: &[serde_json::Value]) -> anyhow::Result<Vec<BoxedTransform>> {
    let mut chain: Vec<BoxedTransform> = Vec::with_capacity(configs.len());
    for cfg in configs {
        let plugin = cfg
            .get("plugin_name")
            .or_else(|| cfg.get("plugin"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        match plugin.as_str() {
            "filter" => {
                let index = cfg
                    .get("field_index")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| {
                        anyhow::anyhow!("filter transform requires integer 'field_index'")
                    })? as usize;
                let op = CompareOp::parse(
                    cfg.get("operator")
                        .and_then(|v| v.as_str())
                        .unwrap_or("not_null"),
                )?;
                let value = cfg.get("value").map(json_scalar_to_field);
                chain.push(Box::new(FilterByIndexTransform::new(index, op, value)));
            }
            other => {
                return Err(anyhow::anyhow!(
                    "unknown transform plugin '{}' (supported: filter)",
                    other
                ));
            }
        }
    }
    Ok(chain)
}

fn json_scalar_to_field(v: &serde_json::Value) -> Field {
    match v {
        serde_json::Value::Bool(b) => Field::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Field::Int64(i)
            } else {
                Field::Float64(n.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(s) => Field::String(s.clone()),
        _ => Field::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_config_map_flattens_dotted_canal_client_keys() {
        // Replicates the sink section of
        // examples/mysql-cdc-to-kafka-canal-client.yaml through the real
        // yaml → json → flat-map chain.
        let yaml = r#"
Kafka:
  bootstrap.servers: "127.0.0.1:9092"
  topic: users-canal-client
  format: canal_client_json
  canal-client.database-name: seatunnel
  canal-client.table-name: users
  canal-client.columns: "id,name,score"
  canal-client.sub-table-fields: >-
    {
      "users": {
        "key": "id",
        "must": { "id": "id", "name": "name" },
        "update": { "score": "score" }
      }
    }
"#;
        let value: serde_json::Value = serde_yaml::from_str(yaml).expect("yaml parses");
        let flat = json_to_config_map(&value["Kafka"]);
        eprintln!("FLAT_MAP_KEYS={:?}", flat.keys().collect::<Vec<_>>());
        for key in [
            "format",
            "canal-client.database-name",
            "canal-client.table-name",
            "canal-client.columns",
            "canal-client.sub-table-fields",
        ] {
            assert!(
                flat.contains_key(key),
                "missing key '{}' in {:?}",
                key,
                flat.keys().collect::<Vec<_>>()
            );
        }
        assert_eq!(flat["format"], "canal_client_json");
    }

    #[test]
    fn test_parse_sink_declarations_shapes() {
        // Array of single-key blocks.
        let array = serde_json::json!([{ "Kafka": { "topic": "a" } }, { "JDBC": {} }]);
        let sinks = parse_sink_declarations(&array).unwrap();
        assert_eq!(sinks.len(), 2);
        assert_eq!(sinks[0].plugin, "Kafka");
        // Multi-key map.
        let map = serde_json::json!({ "Kafka": {}, "JDBC": {} });
        assert_eq!(parse_sink_declarations(&map).unwrap().len(), 2);
        // Legacy single block.
        let single = serde_json::json!({ "Console": {} });
        assert_eq!(parse_sink_declarations(&single).unwrap().len(), 1);
        // Empty list is rejected.
        assert!(parse_sink_declarations(&serde_json::json!([])).is_err());
    }
}
