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

//! MySQL XA exactly-once sink (Java `JdbcExactlyOnceSinkWriter` analog).
//!
//! True two-phase commit on top of MySQL XA:
//! - one XA transaction spans each checkpoint window; the xid is
//!   deterministic (`{prefix}-{pipeline}-{subtask}-cp{window}`) so a
//!   restarted writer can find its predecessors' transactions
//! - `prepare_commit` executes `XA END` + `XA PREPARE` — phase 1 is
//!   durable INSIDE MySQL, surviving writer and engine crashes
//! - the engine persists the checkpoint envelope, then the committer runs
//!   `XA COMMIT 'xid'` from its own connection (phase 2); `XA RECOVER`
//!   makes it idempotent (an already-committed xid simply disappears)
//! - on `open`, leftover prepared xids are settled (`restoreCommit` in
//!   Java): xids for windows already covered by the restored checkpoint
//!   are COMMITTED, everything newer is ROLLED BACK and replayed
//!
//! Replayed rows converge because writes are upserts (`INSERT ... ON
//! DUPLICATE KEY UPDATE`), so even the pre-checkpoint tail is harmless.
//!
//! Requires MySQL (or a MySQL-compatible server with XA support); the
//! target table must exist — this sink performs no DDL.

use std::future::Future;
use std::pin::Pin;

use mysql_async::prelude::*;
use seatunnel_api::ColumnType;
use seatunnel_api::row::{Field, Row, RowKind};
use seatunnel_api::schema::TableSchema;
use seatunnel_api::sink::sink_committer::{CommitterFuture, SinkCommitter};
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::sink::{Sink, SinkWriterContext};
use seatunnel_connector_common::ConnectorConfig;
use serde::{Deserialize, Serialize};

use crate::conn::DbEndpoint;
use crate::url::parse_jdbc_url;
use crate::value::{SqlValue, field_to_sql_value};
use crate::{JdbcUrl, catalog};

/// Commit descriptor: the prepared xid phase 2 must commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XaCommitInfo {
    pub xid: String,
    /// Engine checkpoint id (informational/diagnostics).
    pub checkpoint_id: u64,
    /// Window sequence embedded in the xid; restart recovery compares it
    /// with the restored writer state.
    pub window: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XaAggregatedCommitInfo {
    pub committed: usize,
    pub rows: u64,
}

/// Configuration for the XA sink (subset of the JDBC sink knobs).
#[derive(Debug, Clone)]
pub struct XaSinkConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    pub table: String,
    pub primary_keys: Vec<String>,
    pub batch_size: usize,
    pub enable_upsert: bool,
    /// Xid prefix; the full xid appends `-{pipeline}-{subtask}-cp{window}`.
    pub xid_prefix: String,
    /// Injected by the engine (xid namespace).
    pub context_pipeline: String,
    /// Injected by the engine (xid namespace).
    pub context_subtask: usize,
}

impl XaSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let database = config.get_string("database", "");
        XaSinkConfig {
            url: config.get_string("url", ""),
            username: config.get_string("username", &config.get_string("user", "")),
            password: config.get_string("password", ""),
            database: if database.is_empty() {
                None
            } else {
                Some(database)
            },
            table: config.get_string("table", ""),
            primary_keys: config
                .get_string("primary-keys", &config.get_string("primary_keys", ""))
                .split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect(),
            batch_size: config
                .get_int("batch.size", config.get_int("batch_size", 1000))
                .max(1) as usize,
            enable_upsert: config.get_bool("enable-upsert", config.get_bool("enable_upsert", true)),
            xid_prefix: config.get_string(
                "xa.xid-prefix",
                &config.get_string("xid-prefix", "seatunnel-xa"),
            ),
            context_pipeline: config.get_string("pipeline.name", "p0"),
            context_subtask: config.get_int("subtask.index", 0).max(0) as usize,
        }
    }
}

/// Keep xids SQL-literal safe (they are embedded in `XA '...'` statements).
fn sanitize_component(component: &str) -> String {
    component
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The XA sink connector.
#[derive(Debug, Clone)]
pub struct XaSink {
    config: XaSinkConfig,
}

impl XaSink {
    pub fn new(config: XaSinkConfig) -> Self {
        XaSink { config }
    }

    pub fn from_config(config: &ConnectorConfig) -> Self {
        XaSink::new(XaSinkConfig::from_config(config))
    }

    pub fn config(&self) -> &XaSinkConfig {
        &self.config
    }
}

impl Sink for XaSink {
    type Input = Row;
    type WriterState = serde_json::Value;
    type CommitInfo = XaCommitInfo;
    type AggregatedCommitInfo = XaAggregatedCommitInfo;

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
        Ok(Box::new(XaSinkWriter::new(self.config.clone())))
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
        let mut writer = XaSinkWriter::new(self.config.clone());
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
        Some(Box::new(XaSinkCommitter::new(
            self.config.url.clone(),
            self.config.username.clone(),
            self.config.password.clone(),
        )))
    }
}

/// Window sequence of the currently open (or next) XA transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XaWriterState {
    pub last_committed_window: u64,
    pub written: u64,
}

/// MySQL XA sink writer.
pub struct XaSinkWriter {
    config: XaSinkConfig,
    url: Option<JdbcUrl>,
    /// The single connection all XA statements and data writes run on.
    conn: Option<mysql_async::Conn>,
    table_schema: Option<TableSchema>,
    buffer: Vec<Row>,
    written: u64,
    /// Sequence of the last window whose prepare_commit succeeded; the
    /// open window (if any) is `last_committed_window + 1`.
    last_committed_window: u64,
    txn_open: bool,
    rows_in_txn: u64,
    xid_base: String,
    /// Process incarnation (startup nanos) embedded in xids so a restarted
    /// writer never collides with a zombie session of the previous run: a
    /// SIGKILLed client can leave a half-open TCP session through NAT'd /
    /// port-forwarded links that MySQL still sees holding the ACTIVE XA
    /// transaction under the same name.
    epoch: u64,
}

impl XaSinkWriter {
    pub fn new(config: XaSinkConfig) -> Self {
        let xid_base = format!(
            "{}-{}-{}",
            sanitize_component(&config.xid_prefix),
            sanitize_component(&config.context_pipeline),
            config.context_subtask
        );
        XaSinkWriter {
            config,
            url: None,
            conn: None,
            table_schema: None,
            buffer: Vec::new(),
            written: 0,
            last_committed_window: 0,
            txn_open: false,
            rows_in_txn: 0,
            xid_base,
            epoch: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        }
    }

    /// Restore the last committed window (and totals) from a serialized
    /// `snapshot_state` payload. Determines which prepared xids `open()`
    /// commits versus rolls back.
    pub fn restore_from_state_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let state: XaWriterState =
            serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!("xa writer state: {}", e))?;
        self.last_committed_window = state.last_committed_window;
        self.written = state.written;
        tracing::info!(
            "XaSinkWriter: restored state (last committed window {}, written {})",
            self.last_committed_window,
            self.written
        );
        Ok(())
    }

    fn window_xid(&self, window: u64) -> String {
        format!("{}-r{}-cp{}", self.xid_base, self.epoch, window)
    }

    fn qualified_table(&self) -> String {
        match &self.config.database {
            Some(db) => format!("{}.{}", db, self.config.table),
            None => match &self.url {
                Some(url) if !url.database.is_empty() => {
                    format!("{}.{}", url.database, self.config.table)
                }
                _ => self.config.table.clone(),
            },
        }
    }

    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.conn.is_some() {
            return Ok(());
        }
        let url = parse_jdbc_url(&self.config.url)?;
        if !matches!(
            url.dialect,
            crate::dialect::JdbcDialectKind::MySql | crate::dialect::JdbcDialectKind::TiDB
        ) {
            anyhow::bail!(
                "XaSink requires a MySQL-compatible url (got {:?})",
                url.dialect
            );
        }
        // Discover the target schema through a short-lived pool; the XA
        // work itself runs on one dedicated connection.
        let endpoint =
            DbEndpoint::connect(&url, &self.config.username, &self.config.password, 2).await?;
        let table = self.qualified_table();
        if !catalog::table_exists(&endpoint, &url, &table).await? {
            anyhow::bail!(
                "XaSink target table '{}' does not exist; create it before starting the job \
                 (the XA sink performs no DDL)",
                table
            );
        }
        let schema = catalog::discover_schema(&endpoint, &url, &table)
            .await
            .map_err(|e| {
                anyhow::anyhow!("XaSink schema discovery for '{}' failed: {}", table, e)
            })?;
        let mut schema = schema;
        if schema.primary_key.is_empty() && !self.config.primary_keys.is_empty() {
            schema.primary_key = self.config.primary_keys.clone();
        }
        self.table_schema = Some(schema);
        self.url = Some(url.clone());

        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(&url.host)
            .tcp_port(url.port)
            .user(Some(self.config.username.as_str()))
            .pass(Some(self.config.password.as_str()))
            .db_name(if url.database.is_empty() {
                None
            } else {
                Some(url.database.as_str())
            });
        let conn = mysql_async::Conn::new(opts).await?;
        self.conn = Some(conn);

        // Settle transactions left behind by previous runs.
        self.recover_prepared().await?;
        // Terminate zombie sessions of crashed runs still holding ACTIVE
        // XA transactions on the target database (half-open TCP through
        // port forwarding keeps them alive server-side; their row locks
        // would block replayed writes for the lock-wait timeout).
        self.kill_zombie_sessions().await?;
        Ok(())
    }

    /// Kill foreign sessions holding transactions on the target database.
    /// Requires PROCESS (to see innodb_trx) and CONNECTION_ADMIN/SUPER (to
    /// KILL); both are standard for a dedicated sink user in production.
    async fn kill_zombie_sessions(&mut self) -> anyhow::Result<()> {
        let Some(url) = &self.url else { return Ok(()) };
        let target_db = match &self.config.database {
            Some(db) => db.clone(),
            None => url.database.clone(),
        };
        if target_db.is_empty() {
            return Ok(());
        }
        let sql = format!(
            "SELECT p.id FROM information_schema.processlist p \
             JOIN information_schema.innodb_trx t ON t.trx_mysql_thread_id = p.id \
             WHERE p.id <> CONNECTION_ID() AND p.db = '{}'",
            target_db.replace('\'', "")
        );
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("XaSink not connected"))?;
        let rows: Vec<mysql_async::Row> = match conn.query(&sql).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(
                    "XaSinkWriter: zombie session scan failed (continuing): {}",
                    e
                );
                return Ok(());
            }
        };
        for row in rows {
            if let Some(id) = row.get::<i64, usize>(0) {
                tracing::info!(
                    "XaSinkWriter: killing zombie session {} holding a transaction on {}",
                    id,
                    target_db
                );
                let _ = conn.query_drop(format!("KILL {}", id)).await;
            }
        }
        Ok(())
    }

    /// `XA RECOVER` + settle: xids covered by the restored checkpoint are
    /// committed (finishing their interrupted phase 2), newer ones are
    /// rolled back and will be replayed from the checkpoint.
    async fn recover_prepared(&mut self) -> anyhow::Result<()> {
        let xids = self.xa_recover().await?;
        let mut committed = 0u64;
        let mut rolled_back = 0u64;
        for xid in xids {
            // xid shape: {base}-r{epoch}-cp{window} — the epoch is unique
            // per process, the window is what settlement decides on.
            let Some(window) = xid
                .strip_prefix(&self.xid_base)
                .and_then(|rest| rest.rsplit_once("-cp"))
                .and_then(|(_, window)| window.parse::<u64>().ok())
            else {
                continue; // not ours
            };
            if window <= self.last_committed_window {
                tracing::info!(
                    "XaSinkWriter: committing prepared xid {} (window ≤ {})",
                    xid,
                    self.last_committed_window
                );
                self.xa_exec(&format!("XA COMMIT '{}'", xid)).await?;
                committed += 1;
            } else {
                tracing::info!(
                    "XaSinkWriter: rolling back prepared xid {} (window > {})",
                    xid,
                    self.last_committed_window
                );
                self.xa_exec(&format!("XA ROLLBACK '{}'", xid)).await?;
                rolled_back += 1;
            }
        }
        if committed + rolled_back > 0 {
            tracing::info!(
                "XaSinkWriter: recovery settled {} prepared xid(s): committed={}, rolled back={}",
                committed + rolled_back,
                committed,
                rolled_back
            );
        }
        Ok(())
    }

    /// Run a non-resultset XA statement on the dedicated connection.
    async fn xa_exec(&mut self, sql: &str) -> anyhow::Result<()> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("XaSink not connected"))?;
        conn.query_drop(sql)
            .await
            .map_err(|e| anyhow::anyhow!("xa exec '{}': {}", sql, e))
    }

    /// `XA RECOVER CONVERT INTO`, decoded to gtrid strings.
    async fn xa_recover(&mut self) -> anyhow::Result<Vec<String>> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("XaSink not connected"))?;
        let rows: Vec<mysql_async::Row> = conn
            .query("XA RECOVER")
            .await
            .map_err(|e| anyhow::anyhow!("XA RECOVER: {}", e))?;
        let mut xids = Vec::with_capacity(rows.len());
        for row in rows {
            // Columns: formatID, gtrid_length, bqual_length, data(hex).
            // XA RECOVER columns arrive as Bytes through mysql_async.
            let gtrid_len = match row.get::<mysql_async::Value, usize>(1) {
                Some(mysql_async::Value::Int(n)) => n as usize,
                Some(mysql_async::Value::UInt(n)) => n as usize,
                Some(mysql_async::Value::Bytes(b)) => String::from_utf8_lossy(&b)
                    .trim()
                    .parse::<usize>()
                    .unwrap_or(0),
                _ => 0,
            };
            let data = match row.get::<mysql_async::Value, usize>(3) {
                Some(mysql_async::Value::Bytes(bytes)) => {
                    String::from_utf8_lossy(&bytes).to_string()
                }
                _ => String::new(),
            };
            if let Some(xid) = decode_xa_recover_data(&data, gtrid_len) {
                xids.push(xid);
            }
        }
        Ok(xids)
    }

    async fn conn_exec(&mut self, sql: &str, params: Vec<SqlValue>) -> anyhow::Result<u64> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("XaSink not connected"))?;
        let values: Vec<mysql_async::Value> = params.iter().map(mysql_async::Value::from).collect();
        conn.exec_drop(sql, values)
            .await
            .map_err(|e| anyhow::anyhow!("xa exec: {}", e))?;
        Ok(conn.affected_rows())
    }

    /// Open the XA transaction for the current window if not yet open.
    async fn ensure_txn_open(&mut self) -> anyhow::Result<()> {
        if self.txn_open {
            return Ok(());
        }
        let xid = self.window_xid(self.last_committed_window + 1);
        self.xa_exec(&format!("XA START '{}'", xid)).await?;
        self.txn_open = true;
        tracing::debug!("XaSinkWriter: XA START '{}'", xid);
        Ok(())
    }

    /// Execute buffered rows inside the open XA transaction.
    async fn flush_batch(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_connected().await?;
        self.ensure_txn_open().await?;
        let rows = std::mem::take(&mut self.buffer);
        let url = self.url.clone().expect("connected");
        let dialect = url.dialect;
        let table = self.qualified_table();
        let schema = self.table_schema.clone().expect("schema resolved");
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<ColumnType> = schema
            .columns
            .iter()
            .map(|c| c.column_type.clone())
            .collect();
        let primary_keys: Vec<String> = if !self.config.primary_keys.is_empty() {
            self.config.primary_keys.clone()
        } else {
            schema.primary_key.clone()
        };
        let upsert = self.config.enable_upsert && !primary_keys.is_empty();

        let mut idx = 0;
        while idx < rows.len() {
            let is_delete = rows[idx].kind == RowKind::Delete;
            let mut run_end = idx + 1;
            while run_end < rows.len() && (rows[run_end].kind == RowKind::Delete) == is_delete {
                run_end += 1;
            }
            let run = &rows[idx..run_end];
            if is_delete && !primary_keys.is_empty() {
                let key_positions: Vec<usize> = primary_keys
                    .iter()
                    .map(|pk| {
                        schema
                            .column_index(pk)
                            .ok_or_else(|| anyhow::anyhow!("delete key '{}' not in schema", pk))
                    })
                    .collect::<Result<_, _>>()?;
                let key_types: Vec<ColumnType> = key_positions
                    .iter()
                    .map(|&i| schema.columns[i].column_type.clone())
                    .collect();
                let sql = dialect.build_delete_by_pk(&table, &primary_keys, &key_types);
                for row in run {
                    let params: Vec<SqlValue> = key_positions
                        .iter()
                        .map(|&i| field_to_sql_value(row.fields.get(i).unwrap_or(&Field::Null)))
                        .collect();
                    self.conn_exec(&sql, params).await?;
                    self.written += 1;
                    self.rows_in_txn += 1;
                }
            } else if !is_delete {
                for chunk in run.chunks(self.config.batch_size) {
                    if chunk.is_empty() {
                        continue;
                    }
                    let sql = if upsert {
                        dialect.build_upsert(
                            &table,
                            &columns,
                            &column_types,
                            &primary_keys,
                            chunk.len(),
                        )
                    } else {
                        dialect.build_insert(&table, &columns, &column_types, chunk.len())
                    };
                    let mut params: Vec<SqlValue> = Vec::with_capacity(chunk.len() * columns.len());
                    for row in chunk {
                        for field in &row.fields {
                            params.push(field_to_sql_value(field));
                        }
                    }
                    self.conn_exec(&sql, params).await?;
                    self.written += chunk.len() as u64;
                    self.rows_in_txn += chunk.len() as u64;
                }
            } else {
                tracing::warn!(
                    "XaSinkWriter: dropping {} delete row(s): no primary key configured",
                    run.len()
                );
            }
            idx = run_end;
        }
        Ok(())
    }
}

impl SinkWriter for XaSinkWriter {
    type Input = Row;
    type WriterState = serde_json::Value;
    type CommitInfo = XaCommitInfo;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_connected().await?;
            tracing::info!(
                "XaSinkWriter: ready for table {} (xid base {}, last committed window {})",
                self.config.table,
                self.xid_base,
                self.last_committed_window
            );
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let full = self.buffer.len() + 1 >= self.config.batch_size;
        self.buffer.push(record);
        Box::pin(async move {
            if full {
                self.flush_batch().await?;
            }
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
        checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_connected().await?;
            self.flush_batch().await?;
            if !self.txn_open {
                // No writes this window: nothing to prepare.
                return Ok(Vec::new());
            }
            let window = self.last_committed_window + 1;
            let xid = self.window_xid(window);
            self.xa_exec(&format!("XA END '{}'", xid)).await?;
            self.xa_exec(&format!("XA PREPARE '{}'", xid)).await?;
            self.txn_open = false;
            let info = XaCommitInfo {
                xid: xid.clone(),
                checkpoint_id,
                window,
                rows: self.rows_in_txn,
            };
            self.rows_in_txn = 0;
            self.last_committed_window = window;
            tracing::info!(
                "XaSinkWriter: XA PREPARE '{}' for checkpoint {} ({} row(s))",
                xid,
                checkpoint_id,
                info.rows
            );
            Ok(vec![info])
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = serde_json::json!({
            "last_committed_window": self.last_committed_window,
            "written": self.written,
        });
        Box::pin(async move { serde_json::to_vec(&state).map_err(|e| anyhow::anyhow!("{}", e)) })
    }

    fn poll_flush(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Rows are only visible at XA COMMIT anyway; still execute them
        // eagerly so the buffer stays bounded on idle streams.
        let has_rows = !self.buffer.is_empty();
        Box::pin(async move {
            if has_rows {
                self.flush_batch().await?;
            }
            Ok(())
        })
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            // Tail window with no checkpoint covering it: commit it directly
            // so records are not lost (a restore replays this window and
            // upserts converge). The graceful path already prepared and
            // committed through the committer.
            if self.txn_open {
                if !self.buffer.is_empty() {
                    self.flush_batch().await?;
                }
                let window = self.last_committed_window + 1;
                let xid = self.window_xid(window);
                self.xa_exec(&format!("XA END '{}'", xid)).await?;
                self.xa_exec(&format!("XA COMMIT '{}'", xid)).await?;
                self.txn_open = false;
                self.last_committed_window = window;
                tracing::info!("XaSinkWriter: committed tail window {} on close", window);
            }
            if let Some(conn) = self.conn.take() {
                let _ = conn.disconnect().await;
            }
            tracing::info!("XaSinkWriter: closed, total written {}", self.written);
            Ok(())
        })
    }
}

/// Decode one `XA RECOVER` `data` cell into the gtrid string.
///
/// Plain `XA RECOVER` returns the raw gtrid bytes (this is what MySQL
/// 8.0.46 serves); `XA RECOVER CONVERT INTO` returns a hex string like
/// `0x6162..`. Both are accepted.
fn decode_xa_recover_data(data: &str, gtrid_len: usize) -> Option<String> {
    let bytes: Vec<u8> = if let Some(hex) = data.strip_prefix("0x") {
        if hex.len() % 2 != 0 {
            return None;
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
            .collect::<Option<Vec<u8>>>()?
    } else {
        data.as_bytes().to_vec()
    };
    if bytes.len() < gtrid_len {
        return None;
    }
    String::from_utf8(bytes[..gtrid_len].to_vec()).ok()
}

/// Phase-2 committer: `XA COMMIT` / `XA ROLLBACK` prepared xids from its
/// own connection. Idempotent via `XA RECOVER`.
pub struct XaSinkCommitter {
    url: String,
    username: String,
    password: String,
    conn: Option<mysql_async::Conn>,
}

impl XaSinkCommitter {
    pub fn new(url: String, username: String, password: String) -> Self {
        XaSinkCommitter {
            url,
            username,
            password,
            conn: None,
        }
    }

    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.conn.is_some() {
            return Ok(());
        }
        let url = parse_jdbc_url(&self.url)?;
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(&url.host)
            .tcp_port(url.port)
            .user(Some(self.username.as_str()))
            .pass(Some(self.password.as_str()))
            .db_name(if url.database.is_empty() {
                None
            } else {
                Some(url.database.as_str())
            });
        self.conn = Some(mysql_async::Conn::new(opts).await?);
        Ok(())
    }

    async fn prepared_xids(&mut self) -> anyhow::Result<Vec<String>> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("xa committer not connected"))?;
        let rows: Vec<mysql_async::Row> = conn
            .query("XA RECOVER")
            .await
            .map_err(|e| anyhow::anyhow!("XA RECOVER: {}", e))?;
        let mut xids = Vec::with_capacity(rows.len());
        for row in rows {
            // XA RECOVER columns arrive as Bytes through mysql_async.
            let gtrid_len = match row.get::<mysql_async::Value, usize>(1) {
                Some(mysql_async::Value::Int(n)) => n as usize,
                Some(mysql_async::Value::UInt(n)) => n as usize,
                Some(mysql_async::Value::Bytes(b)) => String::from_utf8_lossy(&b)
                    .trim()
                    .parse::<usize>()
                    .unwrap_or(0),
                _ => 0,
            };
            let data = match row.get::<mysql_async::Value, usize>(3) {
                Some(mysql_async::Value::Bytes(bytes)) => {
                    String::from_utf8_lossy(&bytes).to_string()
                }
                _ => String::new(),
            };
            if let Some(xid) = decode_xa_recover_data(&data, gtrid_len) {
                xids.push(xid);
            }
        }
        Ok(xids)
    }

    async fn xa_statement(&mut self, sql: &str) -> anyhow::Result<()> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("xa committer not connected"))?;
        match conn.query_drop(sql).await {
            Ok(()) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("xa committer '{}': {}", sql, e)),
        }
    }
}

impl SinkCommitter for XaSinkCommitter {
    type CommitInfo = XaCommitInfo;
    type AggregatedCommitInfo = XaAggregatedCommitInfo;

    fn commit(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> CommitterFuture<'_, Self::AggregatedCommitInfo> {
        Box::pin(async move {
            self.ensure_connected().await?;
            let prepared = self.prepared_xids().await?;
            let mut committed = 0usize;
            let mut rows = 0u64;
            for info in commit_infos {
                rows += info.rows;
                if prepared.contains(&info.xid) {
                    self.xa_statement(&format!("XA COMMIT '{}'", info.xid))
                        .await?;
                    committed += 1;
                    tracing::info!(
                        "XaSinkCommitter: committed xid {} (checkpoint {}, {} row(s))",
                        info.xid,
                        info.checkpoint_id,
                        info.rows
                    );
                } else {
                    // Already committed on a previous attempt (idempotent).
                    tracing::debug!(
                        "XaSinkCommitter: xid {} no longer prepared (already committed)",
                        info.xid
                    );
                }
            }
            Ok(XaAggregatedCommitInfo { committed, rows })
        })
    }

    fn abort(&mut self, commit_infos: Vec<Self::CommitInfo>) -> CommitterFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_connected().await?;
            let prepared = self.prepared_xids().await?;
            for info in commit_infos {
                if prepared.contains(&info.xid) {
                    self.xa_statement(&format!("XA ROLLBACK '{}'", info.xid))
                        .await?;
                    tracing::info!("XaSinkCommitter: rolled back prepared xid {}", info.xid);
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid_components_are_sanitized() {
        let config = XaSinkConfig {
            url: "jdbc:mysql://localhost:13306/perf".into(),
            username: "root".into(),
            password: "root".into(),
            database: None,
            table: "t".into(),
            primary_keys: vec!["id".into()],
            batch_size: 100,
            enable_upsert: true,
            xid_prefix: "evil'; DROP".into(),
            context_pipeline: "p0".into(),
            context_subtask: 0,
        };
        let writer = XaSinkWriter::new(config);
        assert_eq!(writer.xid_base, "evil___DROP-p0-0");
        // xids carry the process epoch between the base and the window.
        let xid = writer.window_xid(3);
        assert!(xid.starts_with("evil___DROP-p0-0-r"), "{}", xid);
        assert!(xid.ends_with("-cp3"), "{}", xid);
    }

    #[test]
    fn decode_xa_recover_data_parses_hex_and_raw() {
        // CONVERT INTO form: hex gtrid
        let data = "0x616263";
        assert_eq!(decode_xa_recover_data(data, 3).as_deref(), Some("abc"));
        assert_eq!(decode_xa_recover_data(data, 2).as_deref(), Some("ab"));
        // Plain form: the data column IS the raw gtrid.
        assert_eq!(decode_xa_recover_data("616263", 3).as_deref(), Some("616"));
        // Odd hex length or short payloads are rejected.
        assert!(decode_xa_recover_data("0x616", 1).is_none());
        assert!(decode_xa_recover_data("abc", 8).is_none());
    }

    #[test]
    fn writer_state_roundtrip() {
        let mut writer = XaSinkWriter::new(XaSinkConfig {
            url: "jdbc:mysql://localhost:13306/perf".into(),
            username: "root".into(),
            password: "root".into(),
            database: None,
            table: "t".into(),
            primary_keys: vec![],
            batch_size: 10,
            enable_upsert: true,
            xid_prefix: "t".into(),
            context_pipeline: "p0".into(),
            context_subtask: 0,
        });
        let state = serde_json::to_vec(&XaWriterState {
            last_committed_window: 7,
            written: 1234,
        })
        .unwrap();
        writer.restore_from_state_bytes(&state).unwrap();
        assert_eq!(writer.last_committed_window, 7);
        assert!(writer.window_xid(8).ends_with("-cp8"));
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    async fn mysql_up() -> bool {
        tokio::net::TcpStream::connect("127.0.0.1:13306")
            .await
            .is_ok()
    }

    fn opts() -> mysql_async::OptsBuilder {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(13306)
            .user(Some("root"))
            .pass(Some("root"))
    }

    /// XA RECOVER must list transactions prepared by ANOTHER connection —
    /// this is what writer settlement and the phase-2 committer rely on.
    #[tokio::test]
    async fn xa_recover_lists_prepared_across_connections() {
        if !mysql_up().await {
            eprintln!("SKIP: local mysql not available");
            return;
        }
        let mut a = mysql_async::Conn::new(opts()).await.unwrap();
        let mut b = mysql_async::Conn::new(opts()).await.unwrap();
        let xid = format!("probe-{}", std::process::id());
        let _ = a.query_drop(format!("XA ROLLBACK '{}'", xid)).await;
        a.query_drop(format!("XA START '{}'", xid)).await.unwrap();
        a.query_drop(format!("XA END '{}'", xid)).await.unwrap();
        a.query_drop(format!("XA PREPARE '{}'", xid)).await.unwrap();

        let rows: Vec<mysql_async::Row> = b.query("XA RECOVER").await.unwrap();
        eprintln!("rows: {}", rows.len());
        for r in &rows {
            let vals: Vec<mysql_async::Value> = (0..4)
                .filter_map(|i| r.get::<mysql_async::Value, usize>(i))
                .collect();
            eprintln!("row: {:?}", vals);
        }
        let seen = rows.iter().any(|r| {
            matches!(
                r.get::<mysql_async::Value, usize>(3),
                Some(mysql_async::Value::Bytes(ref bytes)) if String::from_utf8_lossy(bytes) == xid
            )
        });
        let _ = a.query_drop(format!("XA ROLLBACK '{}'", xid)).await;
        assert!(
            seen,
            "XA RECOVER on connection B must list the xid prepared on A"
        );
    }
}
