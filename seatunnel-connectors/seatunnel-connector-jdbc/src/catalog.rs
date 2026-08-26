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

//! Schema discovery over information_schema (MySQL/TiDB) and pg_catalog
//! (Postgres), mirroring the Java connector's catalog layer.

use seatunnel_api::schema::{ColumnDef, TableSchema};

use crate::conn::{DbEndpoint, QueryResult};
use crate::dialect::JdbcDialectKind;
use crate::url::{split_table_name, JdbcUrl};
use crate::value::SqlValue;

/// Discover columns of `table` (optionally `namespace.table`) on the endpoint.
pub async fn discover_columns(
    endpoint: &DbEndpoint,
    url: &JdbcUrl,
    table: &str,
) -> anyhow::Result<Vec<ColumnDef>> {
    let (namespace, table) = split_table_name(table);
    match url.dialect {
        JdbcDialectKind::Postgres => {
            let schema = namespace.unwrap_or_else(|| "public".to_string());
            discover_postgres(endpoint, &schema, &table).await
        }
        JdbcDialectKind::MySql | JdbcDialectKind::TiDB => {
            let db = namespace.unwrap_or_else(|| {
                if url.database.is_empty() {
                    "information_schema".to_string()
                } else {
                    url.database.clone()
                }
            });
            discover_mysql(endpoint, &db, &table).await
        }
    }
}

async fn discover_mysql(
    endpoint: &DbEndpoint,
    database: &str,
    table: &str,
) -> anyhow::Result<Vec<ColumnDef>> {
    let result = endpoint
        .query(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, \
             CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE \
             FROM information_schema.columns \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            &[SqlValue::Str(database.to_string()), SqlValue::Str(table.to_string())],
        )
        .await?;

    let dialect = JdbcDialectKind::MySql.api_dialect();
    let mut columns = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let Some(name) = text_of(row.first().unwrap_or(&SqlValue::Null)) else {
            continue;
        };
        let Some(data_type) = text_of(row.get(1).unwrap_or(&SqlValue::Null)) else {
            continue;
        };
        let nullable = text_of(row.get(2).unwrap_or(&SqlValue::Null))
            .map(|s| s == "YES")
            .unwrap_or(true);
        let column_key =
            text_of(row.get(3).unwrap_or(&SqlValue::Null)).unwrap_or_default();
        let length = row.get(4).and_then(|v| match v {
            SqlValue::Int(n) => Some(*n as u32),
            SqlValue::Str(s) => s.parse().ok(),
            _ => None,
        });
        let scale = row.get(5).and_then(|v| match v {
            SqlValue::Int(n) => Some(*n as i8),
            SqlValue::Str(s) => s.parse().ok(),
            _ => None,
        });
        let column_type = dialect.map_type(&data_type, length, scale);
        columns.push(
            ColumnDef::new(name, column_type)
                .nullable(nullable)
                .with_primary_key(column_key == "PRI")
                .source_type(data_type),
        );
    }
    Ok(columns)
}

fn text_of(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Str(s) => Some(s.clone()),
        SqlValue::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
        SqlValue::Int(v) => Some(v.to_string()),
        SqlValue::UInt(v) => Some(v.to_string()),
        SqlValue::Float(v) => Some(v.to_string()),
        _ => None,
    }
}

async fn discover_postgres(
    endpoint: &DbEndpoint,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<ColumnDef>> {
    let result = endpoint
        .query(
            "SELECT a.attname AS column_name, \
                    t.typname AS data_type, \
                    NOT a.attnotnull AS nullable, \
                    COALESCE((SELECT ix.indisprimary FROM pg_index ix \
                              WHERE ix.indrelid = a.attrelid AND a.attnum = ANY(ix.indkey) \
                              LIMIT 1), false) AS is_primary \
             FROM pg_attribute a \
             JOIN pg_class c ON a.attrelid = c.oid \
             JOIN pg_namespace n ON c.relnamespace = n.oid \
             JOIN pg_type t ON a.atttypid = t.oid \
             WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[
                SqlValue::Str(schema.to_string()),
                SqlValue::Str(table.to_string()),
            ],
        )
        .await?;

    let dialect = JdbcDialectKind::Postgres.api_dialect();
    let mut columns = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let Some(name) = text_of(row.first().unwrap_or(&SqlValue::Null)) else {
            continue;
        };
        let data_type = text_of(row.get(1).unwrap_or(&SqlValue::Null)).unwrap_or_default();
        let nullable = matches!(row.get(2), Some(SqlValue::Bool(true)));
        let is_primary = matches!(row.get(3), Some(SqlValue::Bool(true)));
        let column_type = dialect.map_type(&data_type, None, None);
        columns.push(
            ColumnDef::new(name, column_type)
                .nullable(nullable)
                .with_primary_key(is_primary)
                .source_type(data_type),
        );
    }
    Ok(columns)
}

/// Discover the full table schema including primary keys.
pub async fn discover_schema(
    endpoint: &DbEndpoint,
    url: &JdbcUrl,
    table: &str,
) -> anyhow::Result<TableSchema> {
    let columns = discover_columns(endpoint, url, table).await?;
    if columns.is_empty() {
        anyhow::bail!("table '{}' not found or has no columns", table);
    }
    let (namespace, tbl) = split_table_name(table);
    let identifier = match namespace {
        Some(ns) => format!("{}.{}", ns, tbl),
        None => tbl,
    };
    Ok(TableSchema::new(identifier, columns))
}

/// Check whether a table exists.
pub async fn table_exists(
    endpoint: &DbEndpoint,
    url: &JdbcUrl,
    table: &str,
) -> anyhow::Result<bool> {
    let (namespace, table) = split_table_name(table);
    let sql = match url.dialect {
        JdbcDialectKind::Postgres => {
            let schema = namespace.unwrap_or_else(|| "public".to_string());
            endpoint
                .query(
                    "SELECT 1 FROM pg_tables WHERE schemaname = $1 AND tablename = $2",
                    &[SqlValue::Str(schema), SqlValue::Str(table)],
                )
                .await
        }
        _ => {
            let db = namespace.unwrap_or_else(|| url.database.clone());
            endpoint
                .query(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                    &[SqlValue::Str(db), SqlValue::Str(table)],
                )
                .await
        }
    };
    sql.map(|r: QueryResult| !r.rows.is_empty())
}

/// Count rows (used for split sizing).
pub async fn count_rows(endpoint: &DbEndpoint, dialect: JdbcDialectKind, table: &str) -> anyhow::Result<u64> {
    let quoted = dialect.quote_table(table);
    let result = endpoint
        .query(&format!("SELECT COUNT(*) FROM {}", quoted), &[])
        .await?;
    Ok(result.scalar_i64().unwrap_or(0) as u64)
}
