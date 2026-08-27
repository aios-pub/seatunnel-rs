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

//! JDBC sink: batched insert / native upsert / delete with save modes,
//! auto table creation and mid-stream schema evolution
//! (Java: `JdbcSinkWriter` + `SupportSchemaEvolutionSink`).

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use seatunnel_api::schema::{ColumnDef, TableSchema};
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::sink::{Sink, SinkWriterContext};
use seatunnel_api::{ColumnType, Field, Row, RowKind, SchemaChangeEvent};
use seatunnel_connector_common::ConnectorConfig;

use crate::catalog;
use crate::conn::DbEndpoint;
use crate::url::{parse_jdbc_url, split_table_name, JdbcUrl};
use crate::value::{field_to_sql_value, SqlValue};

/// Startup behavior for the target table
/// (Java: `org.apache.seatunnel.api.sink.SchemaSaveMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaSaveMode {
    RecreateSchema,
    CreateWhenNotExist,
    ErrorWhenNotExist,
    Ignore,
}

impl SchemaSaveMode {
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "recreateschema" => SchemaSaveMode::RecreateSchema,
            "errorwhenschemanotexist" => SchemaSaveMode::ErrorWhenNotExist,
            "ignore" => SchemaSaveMode::Ignore,
            _ => SchemaSaveMode::CreateWhenNotExist,
        }
    }
}

/// Startup behavior for existing data
/// (Java: `org.apache.seatunnel.api.sink.DataSaveMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataSaveMode {
    DropData,
    AppendData,
    ErrorWhenDataExists,
    CustomProcessing,
}

impl DataSaveMode {
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().replace(['-', '_'], "").as_str() {
            "dropdata" => DataSaveMode::DropData,
            "errorwhendataexists" => DataSaveMode::ErrorWhenDataExists,
            "customprocessing" => DataSaveMode::CustomProcessing,
            _ => DataSaveMode::AppendData,
        }
    }
}

/// JDBC sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JdbcSinkConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    pub table: String,
    pub primary_keys: Vec<String>,
    /// Optional explicit column names for positional rows (defaults to
    /// f0..fN when the target table neither exists nor is discoverable).
    pub columns: Vec<String>,
    pub batch_size: usize,
    pub max_retries: u32,
    pub enable_upsert: bool,
    pub schema_save_mode: SchemaSaveMode,
    pub data_save_mode: DataSaveMode,
    pub custom_sql: Option<String>,
}

impl JdbcSinkConfig {
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let schema_save_mode = {
            let v = config.get_string("schema-save-mode", &config.get_string("schema_save_mode", ""));
            if !v.is_empty() {
                SchemaSaveMode::parse(&v)
            } else {
                // Legacy `auto-create-table` and the default both map here.
                SchemaSaveMode::CreateWhenNotExist
            }
        };
        let data_save_mode = DataSaveMode::parse(&config.get_string(
            "data-save-mode",
            &config.get_string("data_save_mode", ""),
        ));
        JdbcSinkConfig {
            url: config.get_string("url", ""),
            username: config
                .get_string("username", &config.get_string("user", "")),
            password: config.get_string("password", ""),
            database: {
                let v = config.get_string("database", "");
                if v.is_empty() { None } else { Some(v) }
            },
            table: config.get_string("table", ""),
            primary_keys: split_csv(&config.get_string(
                "primary-keys",
                &config.get_string("primary_keys", ""),
            )),
            columns: split_csv(&config.get_string("columns", "")),
            batch_size: config
                .get_int("batch.size", config.get_int("batch_size", 1000))
                .max(1) as usize,
            max_retries: config
                .get_int("max-retries", config.get_int("max_retries", 3))
                .max(0) as u32,
            enable_upsert: config.get_bool(
                "enable-upsert",
                config.get_bool("enable_upsert", config.get_bool("upsert.mode", true)),
            ),
            schema_save_mode,
            data_save_mode,
            custom_sql: {
                let v = config.get_string("custom-sql", &config.get_string("custom_sql", ""));
                if v.is_empty() { None } else { Some(v) }
            },
        }
    }
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// JDBC sink writer with batched writes and schema evolution support.
pub struct JdbcSinkWriter {
    config: JdbcSinkConfig,
    provided_schema: Option<TableSchema>,
    url: Option<JdbcUrl>,
    endpoint: Option<DbEndpoint>,
    table_schema: Option<TableSchema>,
    /// Row buffer preserving CDC arrival order (UpdateBefore-delete must
    /// precede its UpdateAfter-insert); flushed in same-kind runs.
    buffer: Vec<Row>,
    save_mode_applied: bool,
    written: u64,
}

impl JdbcSinkWriter {
    pub fn new(config: JdbcSinkConfig, schema: Option<TableSchema>) -> Self {
        JdbcSinkWriter {
            config,
            provided_schema: schema,
            url: None,
            endpoint: None,
            table_schema: None,
            buffer: Vec::new(),
            save_mode_applied: false,
            written: 0,
        }
    }

    /// Fully-qualified table name including the optional database/schema.
    fn qualified_table(&self) -> String {
        match (&self.config.database, split_table_name(&self.config.table).0) {
            (Some(db), _) => format!("{}.{}", db, self.config.table),
            (None, Some(_)) => self.config.table.clone(),
            (None, None) => match &self.url {
                Some(url) if !url.database.is_empty() => {
                    format!("{}.{}", url.database, self.config.table)
                }
                _ => self.config.table.clone(),
            },
        }
    }

    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.endpoint.is_some() {
            return Ok(());
        }
        let url = parse_jdbc_url(&self.config.url)?;
        let endpoint =
            DbEndpoint::connect(&url, &self.config.username, &self.config.password, 4).await?;
        self.url = Some(url.clone());
        self.endpoint = Some(endpoint);

        // Resolve the working schema: explicit schema > discovered table.
        let table = self.qualified_table();
        if let Some(schema) = &self.provided_schema {
            let mut schema = schema.clone();
            if schema.primary_key.is_empty() && !self.config.primary_keys.is_empty() {
                schema.primary_key = self.config.primary_keys.clone();
            }
            self.table_schema = Some(schema);
        } else if catalog::table_exists(self.endpoint.as_ref().unwrap(), &url, &table).await? {
            match catalog::discover_schema(self.endpoint.as_ref().unwrap(), &url, &table).await {
                Ok(schema) => self.table_schema = Some(schema),
                Err(e) => tracing::warn!("JDBC sink: schema discovery failed: {}", e),
            }
        }
        self.apply_save_modes().await?;
        Ok(())
    }

    /// Apply schema / data save modes (Java `SaveModeHandler`).
    async fn apply_save_modes(&mut self) -> anyhow::Result<()> {
        if self.save_mode_applied {
            return Ok(());
        }
        let Some(endpoint) = &self.endpoint else {
            return Ok(());
        };
        let Some(url) = &self.url else {
            return Ok(());
        };
        let table = self.qualified_table();
        let dialect = url.dialect;
        let exists = catalog::table_exists(endpoint, url, &table).await?;

        match self.config.schema_save_mode {
            SchemaSaveMode::ErrorWhenNotExist => {
                if !exists {
                    anyhow::bail!("table '{}' does not exist and schema-save-mode is error-when-not-exist", table);
                }
            }
            SchemaSaveMode::RecreateSchema => {
                if exists {
                    endpoint
                        .exec_best_effort(&format!("DROP TABLE IF EXISTS {}", dialect.quote_table(&table)))
                        .await?;
                }
                if let Some(schema) = &self.table_schema {
                    let ddl = dialect.build_create_table(&self.create_table_schema(schema));
                    endpoint.exec_best_effort(&ddl).await?;
                    tracing::info!("JDBC sink: recreated table {}", table);
                } else {
                    tracing::info!(
                        "JDBC sink: table {} dropped; will be created on first batch",
                        table
                    );
                }
            }
            SchemaSaveMode::CreateWhenNotExist => {
                if !exists {
                    if let Some(schema) = &self.table_schema {
                        let ddl = dialect.build_create_table(&self.create_table_schema(schema));
                        endpoint.exec_best_effort(&ddl).await?;
                        tracing::info!("JDBC sink: auto-created table {}", table);
                    } else {
                        tracing::info!(
                            "JDBC sink: table {} missing; will be created from first batch",
                            table
                        );
                    }
                }
            }
            SchemaSaveMode::Ignore => {}
        }

        let exists_after =
            exists || self.table_schema.is_some() && matches!(self.config.schema_save_mode, SchemaSaveMode::CreateWhenNotExist | SchemaSaveMode::RecreateSchema);
        if exists_after {
            match self.config.data_save_mode {
                DataSaveMode::DropData => {
                    endpoint
                        .exec_best_effort(&format!("TRUNCATE TABLE {}", dialect.quote_table(&table)))
                        .await?;
                }
                DataSaveMode::ErrorWhenDataExists => {
                    if exists {
                        let count = catalog::count_rows(endpoint, dialect, &table).await?;
                        if count > 0 {
                            anyhow::bail!(
                                "table '{}' has {} rows and data-save-mode is error-when-data-exists",
                                table,
                                count
                            );
                        }
                    }
                }
                DataSaveMode::CustomProcessing => {
                    if let Some(sql) = &self.config.custom_sql {
                        endpoint.exec_best_effort(sql).await?;
                    }
                }
                DataSaveMode::AppendData => {}
            }
        }
        self.save_mode_applied = true;
        Ok(())
    }

    /// TableSchema for DDL: renames the identifier to the qualified table
    /// and re-marks primary keys from config.
    fn create_table_schema(&self, schema: &TableSchema) -> TableSchema {
        let mut schema = schema.clone();
        schema.table_identifier = self.qualified_table();
        if !self.config.primary_keys.is_empty() {
            schema.primary_key = self.config.primary_keys.clone();
        }
        schema
    }

    /// Infer a schema from the first buffered row (used when the target
    /// table does not exist and no schema was provided).
    fn infer_schema_from_row(&mut self, row: &Row) -> TableSchema {
        let names: Vec<String> = if !self.config.columns.is_empty() {
            self.config.columns.clone()
        } else {
            (0..row.field_count()).map(|i| format!("f{}", i)).collect()
        };
        let mut columns = Vec::with_capacity(row.field_count());
        for (i, field) in row.fields.iter().enumerate() {
            let name = names.get(i).cloned().unwrap_or_else(|| format!("f{}", i));
            let primary = self.config.primary_keys.iter().any(|pk| pk.eq_ignore_ascii_case(&name));
            columns.push(
                ColumnDef::new(name, column_type_of_field(field))
                    .nullable(true)
                    .with_primary_key(primary),
            );
        }
        TableSchema::new(self.qualified_table(), columns)
    }

    /// Lazy table creation from an inferred schema.
    async fn ensure_table_for_row(&mut self, row: &Row) -> anyhow::Result<()> {
        if self.table_schema.is_none() {
            self.table_schema = Some(self.infer_schema_from_row(row));
        }
        let Some(endpoint) = &self.endpoint else {
            return Ok(());
        };
        let Some(url) = &self.url else {
            return Ok(());
        };
        let table = self.qualified_table();
        if !catalog::table_exists(endpoint, url, &table).await? {
            match self.config.schema_save_mode {
                SchemaSaveMode::ErrorWhenNotExist => {
                    anyhow::bail!("table '{}' does not exist", table);
                }
                SchemaSaveMode::CreateWhenNotExist | SchemaSaveMode::RecreateSchema => {
                    let schema = self.table_schema.clone().expect("schema inferred");
                    let ddl = url.dialect.build_create_table(&self.create_table_schema(&schema));
                    endpoint.exec_best_effort(&ddl).await?;
                    tracing::info!("JDBC sink: auto-created table {} from first batch", table);
                }
                SchemaSaveMode::Ignore => {
                    tracing::warn!("JDBC sink: table {} missing (schema-save-mode=ignore); writes will fail", table);
                }
            }
            self.apply_save_modes().await?;
        }
        Ok(())
    }

    /// Align a row with the working schema: pad missing fields with NULL,
    /// drop extras (upstream schema change without an event).
    fn align_row(&self, row: Row) -> Row {
        let Some(schema) = &self.table_schema else {
            return row;
        };
        let expected = schema.column_count();
        let mut row = row;
        match row.field_count().cmp(&expected) {
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Less => {
                while row.field_count() < expected {
                    row.fields.push(Field::Null);
                }
            }
            std::cmp::Ordering::Greater => {
                row.fields.truncate(expected);
            }
        }
        row
    }

    async fn flush_buffers(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_connected().await?;
        let rows = std::mem::take(&mut self.buffer);

        // Execute in arrival order, batching consecutive rows of the same
        // kind so an UPDATE (delete-before + insert-after) applies in order.
        let mut idx = 0;
        while idx < rows.len() {
            let is_delete = rows[idx].kind == RowKind::Delete;
            let mut run_end = idx + 1;
            while run_end < rows.len() && (rows[run_end].kind == RowKind::Delete) == is_delete {
                run_end += 1;
            }
            let run = &rows[idx..run_end];
            if is_delete {
                self.exec_delete_run(run).await?;
            } else {
                self.exec_insert_run(run).await?;
            }
            idx = run_end;
        }
        Ok(())
    }

    async fn exec_insert_run(&mut self, run: &[Row]) -> anyhow::Result<()> {
        let endpoint = self
            .endpoint
            .clone()
            .ok_or_else(|| anyhow::anyhow!("JDBC sink not connected"))?;
        let url = self
            .url
            .clone()
            .ok_or_else(|| anyhow::anyhow!("JDBC sink not connected"))?;
        let dialect = url.dialect;
        let table = self.qualified_table();
        let schema = self
            .table_schema
            .clone()
            .ok_or_else(|| anyhow::anyhow!("JDBC sink has no schema"))?;
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<ColumnType> =
            schema.columns.iter().map(|c| c.column_type.clone()).collect();
        let primary_keys: Vec<String> = if !self.config.primary_keys.is_empty() {
            self.config.primary_keys.clone()
        } else {
            schema.primary_key.clone()
        };
        let upsert = self.config.enable_upsert && !primary_keys.is_empty();

        let mut written = 0u64;
        for chunk in run.chunks(self.config.batch_size) {
            if chunk.is_empty() {
                continue;
            }
            let sql = if upsert {
                dialect.build_upsert(&table, &columns, &column_types, &primary_keys, chunk.len())
            } else {
                dialect.build_insert(&table, &columns, &column_types, chunk.len())
            };
            let mut params: Vec<SqlValue> = Vec::with_capacity(chunk.len() * columns.len());
            for row in chunk {
                for field in &row.fields {
                    params.push(field_to_sql_value(field));
                }
            }
            exec_with_retry(&endpoint, &sql, &params, self.config.max_retries).await?;
            written += chunk.len() as u64;
        }
        self.written += written;
        Ok(())
    }

    async fn exec_delete_run(&mut self, run: &[Row]) -> anyhow::Result<()> {
        let endpoint = self
            .endpoint
            .clone()
            .ok_or_else(|| anyhow::anyhow!("JDBC sink not connected"))?;
        let url = self
            .url
            .clone()
            .ok_or_else(|| anyhow::anyhow!("JDBC sink not connected"))?;
        let dialect = url.dialect;
        let table = self.qualified_table();
        let schema = self
            .table_schema
            .clone()
            .ok_or_else(|| anyhow::anyhow!("JDBC sink has no schema"))?;
        let primary_keys: Vec<String> = if !self.config.primary_keys.is_empty() {
            self.config.primary_keys.clone()
        } else {
            schema.primary_key.clone()
        };
        if primary_keys.is_empty() {
            tracing::warn!(
                "JDBC sink: dropping {} delete row(s): no primary key configured",
                run.len()
            );
            return Ok(());
        }
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
            exec_with_retry(&endpoint, &sql, &params, self.config.max_retries).await?;
            self.written += 1;
        }
        Ok(())
    }
}

/// Execute with bounded retry + linear backoff (transient failures only).
async fn exec_with_retry(
    endpoint: &DbEndpoint,
    sql: &str,
    params: &[SqlValue],
    max_retries: u32,
) -> anyhow::Result<()> {
    let mut attempt = 0u32;
    loop {
        match endpoint.exec(sql, params).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < max_retries => {
                attempt += 1;
                tracing::warn!(
                    "JDBC sink write failed (attempt {}/{}): {}; retrying",
                    attempt,
                    max_retries,
                    e
                );
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Map a Field variant to the column type used for auto table creation.
fn column_type_of_field(field: &Field) -> ColumnType {
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
            let (digits, scale) = decimal_digits(d);
            ColumnType::Decimal {
                precision: digits.clamp(1, 65) as u8,
                scale: scale.clamp(0, 30),
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
        Field::Array(_) => ColumnType::Array {
            element_type: Box::new(ColumnType::String),
        },
        Field::Row(_) => ColumnType::String,
    }
}

fn decimal_digits(d: &bigdecimal::BigDecimal) -> (u64, i8) {
    let stringified = d.to_string();
    let (int_part, frac_part) = match stringified.split_once('.') {
        Some((i, f)) => (i.trim_start_matches('0'), f),
        None => (stringified.trim_start_matches('0'), ""),
    };
    let int_digits = int_part.len() as u64;
    let frac_digits = frac_part.len() as i8;
    (int_digits + frac_digits as u64, frac_digits)
}

impl SinkWriter for JdbcSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Err(e) = self.ensure_connected().await {
                tracing::warn!("JDBC sink: connection deferred to first write: {}", e);
            }
            Ok(())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let row = match record.kind {
                RowKind::Insert | RowKind::UpdateAfter => Some(record),
                // UpdateBefore rows are not written (Java behavior).
                RowKind::UpdateBefore => None,
                RowKind::Delete => Some(record),
            };
            let Some(row) = row else {
                return Ok(());
            };
            if self.table_schema.is_none() || self.endpoint.is_none() {
                self.ensure_connected().await?;
                self.ensure_table_for_row(&row).await?;
            }
            let row = self.align_row(row);
            self.buffer.push(row);
            if self.buffer.len() >= self.config.batch_size {
                self.flush_buffers().await?;
            }
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
        _checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        Box::pin(async move {
            if self.endpoint.is_some() {
                self.flush_buffers().await?;
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
            if self.endpoint.is_some() {
                self.flush_buffers().await?;
            }
            Ok(())
        })
    }

    fn apply_schema_change(
        &mut self,
        event: &SchemaChangeEvent,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let event = event.clone();
        Box::pin(async move {
            // Flush everything buffered under the old schema first.
            if self.endpoint.is_some() {
                self.flush_buffers().await?;
            }

            // Update the working schema regardless of DDL outcome so that
            // subsequent statement generation matches the new shape.
            let mut new_schema = self
                .table_schema
                .clone()
                .or_else(|| self.provided_schema.clone());

            // Auto-created sinks know columns positionally (f0..fN);
            // translate source column names by ordinal. The translated
            // changes drive BOTH the DDL and the working-schema mutation so
            // the statement column list matches the physical table.
            let changes: Vec<seatunnel_api::SchemaChange> = event
                .changes
                .iter()
                .map(|change| match &self.table_schema {
                    Some(schema) if schema.get_column(change.column_name()).is_none() => {
                        seatunnel_api::schema::translate_positional(change)
                    }
                    _ => change.clone(),
                })
                .collect();

            if let Some(endpoint) = &self.endpoint {
                if let Some(url) = &self.url {
                    let dialect = url.dialect;
                    let table = self.qualified_table();
                    for change in &changes {
                        let statements = dialect.build_schema_change(&table, change);
                        for stmt in statements {
                            match endpoint.exec(&stmt, &[]).await {
                                Ok(_) => {
                                    tracing::info!("JDBC sink schema change applied: {}", stmt);
                                }
                                Err(e) if is_benign_ddl_error(&e.to_string()) => {
                                    tracing::info!(
                                        "JDBC sink schema change already applied: {} ({})",
                                        stmt,
                                        e
                                    );
                                }
                                Err(e) => {
                                    anyhow::bail!("schema change DDL failed ({}): {}", stmt, e)
                                }
                            }
                        }
                    }
                }
            } else {
                tracing::warn!(
                    "JDBC sink not connected; schema change recorded but no DDL executed"
                );
            }

            if let Some(schema) = &mut new_schema {
                let translated_event = SchemaChangeEvent::new(event.table.clone(), changes);
                if let Err(e) = schema.apply_schema_change_event(&translated_event) {
                    tracing::warn!("JDBC sink schema event not applicable: {}", e);
                }
                self.table_schema = Some(schema.clone());
            }
            Ok(())
        })
    }
}

/// Errors from re-applying an already-applied DDL (at-least-once replay):
/// duplicate ADD COLUMN, or DROP/RENAME/MODIFY of an already-removed
/// column (Postgres SQLSTATE 42703).
fn is_benign_ddl_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    if lower.contains("duplicate column") || lower.contains("42701") {
        return true;
    }
    if lower.contains("already exists") && lower.contains("column") {
        return true;
    }
    if lower.contains("does not exist") && lower.contains("column") {
        return true;
    }
    false
}

/// JDBC sink connector (Java: `JdbcSink`).
#[derive(Debug, Clone)]
pub struct JdbcSink {
    pub config: JdbcSinkConfig,
    pub schema: Option<TableSchema>,
}

impl JdbcSink {
    pub fn new(config: JdbcSinkConfig, schema: Option<TableSchema>) -> Self {
        JdbcSink { config, schema }
    }
}

impl Sink for JdbcSink {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = String;
    type AggregatedCommitInfo = Vec<String>;

    fn create_writer(
        &self,
        _writer_context: &SinkWriterContext,
    ) -> anyhow::Result<Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>>
    {
        Ok(Box::new(JdbcSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
        )))
    }

    fn restore_writer(
        &self,
        _writer_context: &SinkWriterContext,
        _states: &[Vec<u8>],
    ) -> anyhow::Result<Box<dyn SinkWriter<Input = Self::Input, WriterState = Self::WriterState, CommitInfo = Self::CommitInfo>>>
    {
        Ok(Box::new(JdbcSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
        )))
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
    use seatunnel_connector_common::ConnectorConfig;

    fn config(pairs: &[(&str, &str)]) -> JdbcSinkConfig {
        let props: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        JdbcSinkConfig::from_config(&ConnectorConfig::new(props))
    }

    #[test]
    fn test_config_parsing() {
        let cfg = config(&[
            ("url", "jdbc:mysql://127.0.0.1:3306/seatunnel"),
            ("username", "root"),
            ("password", "pass"),
            ("table", "users_sink"),
            ("primary-keys", "id,name"),
            ("batch.size", "500"),
            ("enable-upsert", "false"),
        ]);
        assert_eq!(cfg.table, "users_sink");
        assert_eq!(cfg.primary_keys, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(cfg.batch_size, 500);
        assert!(!cfg.enable_upsert);
        assert_eq!(cfg.schema_save_mode, SchemaSaveMode::CreateWhenNotExist);
        assert_eq!(cfg.data_save_mode, DataSaveMode::AppendData);
    }

    #[test]
    fn test_save_mode_parsing() {
        assert_eq!(
            SchemaSaveMode::parse("CREATE_SCHEMA_WHEN_NOT_EXIST"),
            SchemaSaveMode::CreateWhenNotExist
        );
        assert_eq!(SchemaSaveMode::parse("recreate-schema"), SchemaSaveMode::RecreateSchema);
        assert_eq!(
            DataSaveMode::parse("DROP_DATA"),
            DataSaveMode::DropData
        );
    }

    #[test]
    fn test_column_type_inference() {
        assert_eq!(column_type_of_field(&Field::Int32(1)), ColumnType::Int32);
        assert_eq!(column_type_of_field(&Field::Null), ColumnType::String);
        let d: bigdecimal::BigDecimal = "123.45".parse().unwrap();
        match column_type_of_field(&Field::Decimal(d)) {
            ColumnType::Decimal { precision, scale } => {
                assert_eq!((precision, scale), (5, 2));
            }
            other => panic!("unexpected {:?}", other),
        }
    }

    #[test]
    fn test_decimal_digits() {
        let d: bigdecimal::BigDecimal = "0.001".parse().unwrap();
        assert_eq!(decimal_digits(&d), (3, 3));
        let d: bigdecimal::BigDecimal = "100".parse().unwrap();
        assert_eq!(decimal_digits(&d), (3, 0));
    }

    #[test]
    fn test_benign_ddl_errors() {
        assert!(is_benign_ddl_error(
            "Duplicate column name 'email'"
        ));
        assert!(is_benign_ddl_error(
            "column \"email\" of relation \"t\" does not exist" // drop column replay
        ));
        assert!(!is_benign_ddl_error("syntax error"));
    }
}
