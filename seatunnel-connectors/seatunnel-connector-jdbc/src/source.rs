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

//! JDBC source: bounded snapshot reads with parallel keyset splits
//! (Java: `JdbcSource` + `FixedChunkSplitter`).

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

use seatunnel_api::schema::TableSchema;
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::source::source_split::SourceSplit;
use seatunnel_api::{Field, Row, RowKind};
use seatunnel_connector_common::ConnectorConfig;

use crate::catalog;
use crate::conn::DbEndpoint;
use crate::dialect::JdbcDialectKind;
use crate::url::{parse_jdbc_url, JdbcUrl};
use crate::value::{field_to_sql_value, sql_value_to_field, SqlValue};

/// JDBC source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcSourceConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    /// Table to read (`db.table` or plain `table`).
    pub table: String,
    /// Optional custom query; when set, partitioning is disabled and only
    /// subtask 0 executes it.
    pub query: Option<String>,
    /// Split column; defaults to the primary key when discoverable.
    pub partition_column: Option<String>,
    /// Number of partitions; defaults to parallelism.
    pub partition_num: Option<usize>,
    /// Page size for keyset pagination.
    pub fetch_size: usize,
    pub parallelism: usize,
    pub subtask_index: usize,
    pub subtask_count: usize,
}

impl JdbcSourceConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let query = config.get_string("query", "");
        JdbcSourceConfig {
            url: config.get_string("url", ""),
            username: config
                .get_string("username", &config.get_string("user", "")),
            password: config.get_string("password", ""),
            table: config.get_string("table", ""),
            query: if query.is_empty() { None } else { Some(query) },
            partition_column: {
                let v = config.get_string("partition.column", "");
                let v2 = config.get_string("partition_column", "");
                if !v.is_empty() {
                    Some(v)
                } else if !v2.is_empty() {
                    Some(v2)
                } else {
                    None
                }
            },
            partition_num: {
                let v = config.get_int("partition.num", -1);
                let v2 = config.get_int("partition_num", -1);
                if v > 0 {
                    Some(v as usize)
                } else if v2 > 0 {
                    Some(v2 as usize)
                } else {
                    None
                }
            },
            fetch_size: config
                .get_int("fetch.size", config.get_int("batch.size", config.get_int("fetch_size", 1024)))
                .max(1) as usize,
            parallelism: config.get_int("parallelism", 4).max(1) as usize,
            subtask_index: config.get_int("subtask.index", 0).max(0) as usize,
            subtask_count: config.get_int("subtask.count", 1).max(1) as usize,
        }
    }
}

/// Keyset range split owned by this subtask.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcRange {
    pub start: i64,
    pub end: i64,
}

/// Checkpoint state of the JDBC source reader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcSourceState {
    pub range_idx: usize,
    pub last_pk: i64,
    pub done: bool,
}

/// JDBC source reader: streams rows of the configured table/query using
/// keyset pagination when the split column is an integer, offset paging
/// otherwise.
pub struct JdbcSourceReader {
    config: JdbcSourceConfig,
    schema: Option<TableSchema>,
    initial_schema: Option<TableSchema>,
    endpoint: Option<DbEndpoint>,
    url: Option<JdbcUrl>,
    partition_column: Option<String>,
    ranges: Vec<JdbcRange>,
    range_idx: usize,
    last_pk: i64,
    offset: u64,
    /// FIFO page buffer; `VecDeque` keeps per-record pops O(1) without the
    /// deep clone the old `Vec::remove(0)` path paid on every record.
    buffer: std::collections::VecDeque<Row>,
    done: bool,
    restored: Option<JdbcSourceState>,
}

impl JdbcSourceReader {
    pub fn new(config: JdbcSourceConfig, schema: Option<TableSchema>) -> Self {
        // The schema provided at construction is used as a fallback when
        // the target cannot be discovered.
        let initial_schema = schema;
        JdbcSourceReader {
            config,
            schema: None,
            initial_schema,
            endpoint: None,
            url: None,
            partition_column: None,
            ranges: Vec::new(),
            range_idx: 0,
            last_pk: 0,
            offset: 0,
            buffer: std::collections::VecDeque::new(),
            done: false,
            restored: None,
        }
    }

    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: JdbcSourceState = serde_json::from_slice(bytes)?;
        self.restored = Some(state);
        Ok(())
    }

    async fn ensure_initialized(&mut self) -> anyhow::Result<()> {
        if self.endpoint.is_some() {
            return Ok(());
        }
        let url = parse_jdbc_url(&self.config.url)?;
        let endpoint = DbEndpoint::connect(&url, &self.config.username, &self.config.password, 4).await?;

        let (schema, partition_column) = if self.config.query.is_some() {
            // Custom query: schema discovered lazily from the result set.
            (None, None)
        } else {
            let schema = catalog::discover_schema(&endpoint, &url, &self.config.table).await?;
            let pk = schema
                .primary_key
                .first()
                .cloned()
                .or_else(|| schema.columns.first().map(|c| c.name.clone()));
            (Some(schema), pk)
        };

        self.partition_column = self
            .config
            .partition_column
            .clone()
            .or(partition_column);
        self.schema = schema.or(self.initial_schema.clone());
        self.url = Some(url.clone());
        self.endpoint = Some(endpoint);

        // Compute ranges: query mode → single split on subtask 0.
        if self.config.query.is_some() {
            if self.config.subtask_index == 0 {
                self.ranges = vec![JdbcRange { start: 0, end: i64::MAX }];
            }
            return Ok(());
        }

        let dialect = url.dialect;
        let split_col = self.partition_column.clone();
        let table = self.config.table.clone();
        let is_integer_pk = self
            .schema
            .as_ref()
            .and_then(|s| s.get_column(split_col.as_deref().unwrap_or("")))
            .map(|c| {
                matches!(
                    c.column_type,
                    seatunnel_api::ColumnType::Int8
                        | seatunnel_api::ColumnType::Int16
                        | seatunnel_api::ColumnType::Int32
                        | seatunnel_api::ColumnType::Int64
                )
            })
            .unwrap_or(false);

        if !is_integer_pk {
            // Fallback: a single full-table split read with offset paging,
            // executed by subtask 0 only.
            if self.config.subtask_index == 0 {
                self.ranges = vec![JdbcRange { start: i64::MIN, end: i64::MAX }];
            }
            return Ok(());
        }

        let quoted = dialect.quote_table(&table);
        let col = dialect.quote(split_col.as_deref().unwrap_or("id"));
        let min_max = self
            .endpoint
            .as_ref()
            .expect("endpoint")
            .query(
                &format!("SELECT MIN({c}), MAX({c}) FROM {t}", c = col, t = quoted),
                &[],
            )
            .await?;
        let min = min_max.rows.first().and_then(|r| r.first()).and_then(|v| match v {
            SqlValue::Int(i) => Some(*i),
            SqlValue::UInt(u) => Some(*u as i64),
            _ => None,
        });
        let max = min_max.rows.first().and_then(|r| r.get(1)).and_then(|v| match v {
            SqlValue::Int(i) => Some(*i),
            SqlValue::UInt(u) => Some(*u as i64),
            _ => None,
        });
        let (Some(min), Some(max)) = (min, max) else {
            // Empty table.
            self.ranges = Vec::new();
            self.done = true;
            return Ok(());
        };

        let partitions = self
            .config
            .partition_num
            .unwrap_or(self.config.subtask_count.max(1));
        let span = (max - min + 1) as f64;
        let chunk = ((span / partitions as f64).ceil() as i64).max(1);
        let mut ranges = Vec::new();
        let mut start = min;
        while start <= max {
            let end = start.saturating_add(chunk).min(max + 1);
            ranges.push(JdbcRange { start, end });
            start = end;
        }
        // Deterministic split→subtask assignment: range i → subtask i % count.
        self.ranges = ranges
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % self.config.subtask_count == self.config.subtask_index)
            .map(|(_, r)| r)
            .collect();
        // Start the keyset cursor before the first range's start so each
        // subtask reads only its own slice.
        if let Some(first) = self.ranges.first() {
            self.last_pk = first.start.saturating_sub(1);
        }

        tracing::info!(
            "JDBC source: {} split(s) assigned to subtask {} (partition column: {})",
            self.ranges.len(),
            self.config.subtask_index,
            self.partition_column.as_deref().unwrap_or("?")
        );

        // Apply restored checkpoint state.
        if let Some(state) = self.restored.take() {
            self.range_idx = state.range_idx.min(self.ranges.len());
            self.last_pk = state.last_pk;
            self.done = state.done || self.ranges.is_empty();
        } else if self.ranges.is_empty() {
            self.done = true;
        }
        Ok(())
    }

    /// SELECT list with per-dialect casting (Postgres temporal/numeric/json
    /// columns are cast to text for uniform decoding).
    fn select_list(&self, dialect: JdbcDialectKind) -> String {
        let Some(schema) = &self.schema else {
            return "*".to_string();
        };
        if !matches!(dialect, JdbcDialectKind::Postgres) {
            return schema
                .columns
                .iter()
                .map(|c| dialect.quote(&c.name))
                .collect::<Vec<_>>()
                .join(", ");
        }
        schema
            .columns
            .iter()
            .map(|c| {
                let native = matches!(
                    c.column_type,
                    seatunnel_api::ColumnType::Bool
                        | seatunnel_api::ColumnType::Int8
                        | seatunnel_api::ColumnType::Int16
                        | seatunnel_api::ColumnType::Int32
                        | seatunnel_api::ColumnType::Int64
                        | seatunnel_api::ColumnType::Float32
                        | seatunnel_api::ColumnType::Float64
                        | seatunnel_api::ColumnType::Bytes
                );
                if native {
                    dialect.quote(&c.name)
                } else {
                    format!("{}::text AS {}", dialect.quote(&c.name), dialect.quote(&c.name))
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn fetch_next_page(&mut self) -> anyhow::Result<bool> {
        let Some(endpoint) = &self.endpoint else {
            return Ok(false);
        };
        let Some(url) = &self.url else {
            return Ok(false);
        };
        let dialect = url.dialect;

        if let Some(query) = &self.config.query {
            if !self.buffer.is_empty() || self.offset > 0 {
                return Ok(false);
            }
            let result = endpoint.query(query, &[]).await?;
            self.buffer = rows_to_records(&result, self.schema.as_ref(), dialect).into();
            self.offset = 1;
            return Ok(!self.buffer.is_empty());
        }

        if self.range_idx >= self.ranges.len() {
            self.done = true;
            return Ok(false);
        }
        let range = self.ranges[self.range_idx].clone();
        let fetch = self.config.fetch_size;
        let table = self.config.table.clone();
        let quoted_table = dialect.quote_table(&table);
        let select_list = self.select_list(dialect);

        let (sql, params): (String, Vec<SqlValue>) = if range.start == i64::MIN {
            // Non-keyset fallback: offset paging over the full table.
            let order_col = self
                .partition_column
                .clone()
                .unwrap_or_else(|| "1".to_string());
            let order = if order_col == "1" {
                order_col.clone()
            } else {
                dialect.quote(&order_col)
            };
            (
                format!(
                    "SELECT {cols} FROM {t} ORDER BY {o} LIMIT {limit} OFFSET {offset}",
                    cols = select_list,
                    t = quoted_table,
                    o = order,
                    limit = fetch,
                    offset = self.offset
                ),
                vec![],
            )
        } else {
            let col = dialect.quote(self.partition_column.as_deref().unwrap_or("id"));
            let pk_param = field_to_sql_value(&Field::Int64(self.last_pk));
            (
                format!(
                    "SELECT {cols} FROM {t} WHERE {c} > ? AND {c} < ? ORDER BY {c} ASC LIMIT {limit}",
                    cols = select_list,
                    t = quoted_table,
                    c = col,
                    limit = fetch
                ),
                vec![pk_param, field_to_sql_value(&Field::Int64(range.end))],
            )
        };

        let result = endpoint.query(&sql, &params).await?;
        let page_len = result.rows.len();
        // Track keyset cursor from the partition column position.
        let pk_pos = self
            .schema
            .as_ref()
            .and_then(|s| {
                self.partition_column
                    .as_ref()
                    .and_then(|c| s.column_index(c))
            })
            .or_else(|| {
                // Fall back to the position of the column in the result set.
                result
                    .columns
                    .iter()
                    .position(|c| Some(c) == self.partition_column.as_ref())
            });
        self.buffer = rows_to_records(&result, self.schema.as_ref(), dialect).into();
        if let Some(pos) = pk_pos {
            if let Some(last) = result.rows.last() {
                if let Some(v) = last.get(pos) {
                    self.last_pk = match v {
                        SqlValue::Int(i) => *i,
                        SqlValue::UInt(u) => *u as i64,
                        _ => self.last_pk,
                    };
                }
            }
        }
        self.offset += page_len as u64;

        if page_len < fetch {
            // Range exhausted → advance to the next one.
            self.range_idx += 1;
            self.last_pk = self.ranges.get(self.range_idx).map(|r| r.start).unwrap_or(0) - 1;
            self.offset = 0;
        }
        Ok(page_len > 0)
    }
}

fn rows_to_records(
    result: &crate::conn::QueryResult,
    schema: Option<&TableSchema>,
    dialect: JdbcDialectKind,
) -> Vec<Row> {
    let _ = dialect;
    let types: Vec<seatunnel_api::ColumnType> = match schema {
        Some(s) => s.columns.iter().map(|c| c.column_type.clone()).collect(),
        None => Vec::new(),
    };
    let mut rows = Vec::with_capacity(result.rows.len());
    for raw in &result.rows {
        let count = raw.len();
        let mut row = Row::new(RowKind::Insert, count);
        for i in 0..count {
            let ct = types
                .get(i)
                .cloned()
                .unwrap_or(seatunnel_api::ColumnType::String);
            let field = match raw.get(i) {
                Some(v) => sql_value_to_field(v, &ct),
                None => Field::Null,
            };
            row.set(i, field);
        }
        rows.push(row);
    }
    rows
}

/// Opaque split handle (the engine lets readers self-enumerate).
#[derive(Debug, Clone)]
pub struct JdbcSplit {
    pub id: String,
}

impl SourceSplit for JdbcSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

impl SourceReader for JdbcSourceReader {
    type Output = Row;
    type Split = JdbcSplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Err(e) = self.ensure_initialized().await {
                tracing::warn!("JDBC source init deferred: {}", e);
            }
            Ok(())
        })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if self.done && self.buffer.is_empty() {
                return Ok(PollResult::EOF);
            }
            if let Some(row) = self.buffer.pop_front() {
                return Ok(PollResult::Record(row));
            }
            self.ensure_initialized().await?;
            if self.done && self.buffer.is_empty() {
                return Ok(PollResult::EOF);
            }
            if self.fetch_next_page().await? {
                if let Some(row) = self.buffer.pop_front() {
                    return Ok(PollResult::Record(row));
                }
            }
            if self.done && self.buffer.is_empty() {
                return Ok(PollResult::EOF);
            }
            Ok(PollResult::Empty)
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = JdbcSourceState {
            range_idx: self.range_idx,
            last_pk: self.last_pk,
            done: self.done,
        };
        Box::pin(async move { Ok(serde_json::to_vec(&state)?) })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}

    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
