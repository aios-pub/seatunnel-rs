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

use super::{ColumnDef, TableSchema};
use crate::row::ColumnType;
use std::future::Future;
use std::pin::Pin;

/// Trait for database type mapping. Dyn-compatible (no async methods).
pub trait DatabaseDialect: Send + Sync {
    fn map_type(&self, db_type: &str, length: Option<u32>, scale: Option<i8>) -> ColumnType;
    fn build_create_table(&self, schema: &TableSchema) -> String;
    fn dialect_name(&self) -> &str;
}

/// Async schema discovery (returned as Pin<Box<dyn Future>> for dyn compatibility).
pub trait SchemaDiscovery: Send + Sync {
    fn discover_columns(&self, database: &str, table: &str) -> DiscoverColumnsFuture<'_>;
}

/// Boxed future returned by [`SchemaDiscovery::discover_columns`].
pub type DiscoverColumnsFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Vec<ColumnDef>, Box<dyn std::error::Error + Send + Sync>>> + 'a>,
>;

#[allow(dead_code)]
pub struct MySqlDialect;
#[allow(dead_code)]
pub struct PostgresDialect;
#[allow(dead_code)]
pub struct TiDbDialect;

impl DatabaseDialect for MySqlDialect {
    fn map_type(&self, db_type: &str, length: Option<u32>, scale: Option<i8>) -> ColumnType {
        let dt = db_type.to_lowercase();
        if dt == "bool" || dt == "boolean" {
            return ColumnType::Bool;
        }
        match dt.as_str() {
            "tinyint" => {
                if length == Some(1) {
                    ColumnType::Bool
                } else {
                    ColumnType::Int8
                }
            }
            "smallint" | "smallint unsigned" => ColumnType::Int16,
            "mediumint" | "mediumint unsigned" | "int" | "integer" | "year" => ColumnType::Int32,
            "bigint" | "bigint unsigned" => ColumnType::Int64,
            "float" => ColumnType::Float32,
            "double" | "double precision" => ColumnType::Float64,
            "decimal" | "numeric" => ColumnType::Decimal {
                precision: length.unwrap_or(10) as u8,
                scale: scale.unwrap_or(0),
            },
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "json"
            | "enum" | "set" | "cidr" | "inet" | "macaddr" | "uuid" => ColumnType::String,
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit"
            | "geometry" | "point" | "linestring" | "polygon" | "multipoint"
            | "multilinestring" | "multipolygon" | "geomcollection" => ColumnType::Bytes,
            "date" => ColumnType::Date,
            "time" => ColumnType::Time,
            "datetime" => ColumnType::DateTime,
            "timestamp" => ColumnType::TimestampTz,
            _ => ColumnType::String,
        }
    }
    fn build_create_table(&self, schema: &TableSchema) -> String {
        let cols: Vec<String> = schema
            .columns
            .iter()
            .map(|c| {
                let mut s = format!("`{}` {}", c.name, c.column_type);
                if !c.nullable {
                    s.push_str(" NOT NULL");
                }
                s
            })
            .collect();
        let mut sql = format!("CREATE TABLE `{}` (", schema.table_identifier);
        sql.push_str(&cols.join(", "));
        if !schema.primary_key.is_empty() {
            let pk: Vec<String> = schema
                .primary_key
                .iter()
                .map(|n| format!("`{}`", n))
                .collect();
            sql.push_str(&format!(", PRIMARY KEY ({})", pk.join(", ")));
        }
        sql.push(')');
        sql
    }
    fn dialect_name(&self) -> &str {
        "mysql"
    }
}

impl SchemaDiscovery for MySqlDialect {
    fn discover_columns(&self, _database: &str, _table: &str) -> DiscoverColumnsFuture<'_> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl DatabaseDialect for PostgresDialect {
    fn map_type(&self, db_type: &str, length: Option<u32>, scale: Option<i8>) -> ColumnType {
        match db_type {
            "bool" | "boolean" => ColumnType::Bool,
            "smallint" | "smallserial" | "int2" => ColumnType::Int16,
            "integer" | "serial" | "int4" => ColumnType::Int32,
            "bigint" | "bigserial" | "int8" => ColumnType::Int64,
            "real" | "float4" => ColumnType::Float32,
            "double precision" | "float8" => ColumnType::Float64,
            "numeric" | "decimal" => ColumnType::Decimal {
                precision: length.unwrap_or(10) as u8,
                scale: scale.unwrap_or(0),
            },
            "money" => ColumnType::Decimal {
                precision: 30,
                scale: 2,
            },
            "character" | "character varying" | "text" | "uuid" | "json" | "jsonb" | "xml"
            | "inet" | "cidr" | "macaddr" | "interval" | "name" | "regtype" => ColumnType::String,
            "bytea" => ColumnType::Bytes,
            "date" => ColumnType::Date,
            "time" | "timetz" => ColumnType::Time,
            "timestamp" => ColumnType::DateTime,
            "timestamptz" => ColumnType::TimestampTz,
            t if t.starts_with('_') => {
                let elem = self.map_type(&t[1..], length, scale);
                ColumnType::Array {
                    element_type: Box::new(elem),
                }
            }
            "geometry" | "geography" => ColumnType::Bytes,
            _ => ColumnType::String,
        }
    }
    fn build_create_table(&self, schema: &TableSchema) -> String {
        let cols: Vec<String> = schema
            .columns
            .iter()
            .map(|c| {
                let mut s = format!("\"{}\" {}", c.name, c.column_type);
                if !c.nullable {
                    s.push_str(" NOT NULL");
                }
                s
            })
            .collect();
        let mut sql = format!("CREATE TABLE \"{}\" (", schema.table_identifier);
        sql.push_str(&cols.join(", "));
        if !schema.primary_key.is_empty() {
            let pk: Vec<String> = schema
                .primary_key
                .iter()
                .map(|n| format!("\"{}\"", n))
                .collect();
            sql.push_str(&format!(", PRIMARY KEY ({})", pk.join(", ")));
        }
        sql.push(')');
        sql
    }
    fn dialect_name(&self) -> &str {
        "postgres"
    }
}

impl SchemaDiscovery for PostgresDialect {
    fn discover_columns(&self, _database: &str, _table: &str) -> DiscoverColumnsFuture<'_> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl DatabaseDialect for TiDbDialect {
    fn map_type(&self, db_type: &str, length: Option<u32>, scale: Option<i8>) -> ColumnType {
        MySqlDialect.map_type(db_type, length, scale)
    }
    fn build_create_table(&self, schema: &TableSchema) -> String {
        MySqlDialect.build_create_table(schema)
    }
    fn dialect_name(&self) -> &str {
        "tidb"
    }
}

impl SchemaDiscovery for TiDbDialect {
    fn discover_columns(&self, database: &str, table: &str) -> DiscoverColumnsFuture<'_> {
        MySqlDialect.discover_columns(database, table)
    }
}
