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

/// Trait for database type mapping and DDL generation. Dyn-compatible
/// (no async methods).
pub trait DatabaseDialect: Send + Sync {
    fn map_type(&self, db_type: &str, length: Option<u32>, scale: Option<i8>) -> ColumnType;
    fn build_create_table(&self, schema: &TableSchema) -> String;
    fn dialect_name(&self) -> &str;

    /// Quote an identifier (backticks for MySQL family, double quotes for
    /// PostgreSQL family).
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }

    /// Reverse of `map_type`: the concrete SQL type to use for a column.
    /// Used by auto-create-table and `ALTER TABLE` schema evolution.
    fn sql_type_for(&self, column: &ColumnDef) -> String;

    /// `ALTER TABLE ... ADD COLUMN` statement(s).
    fn build_add_column(&self, table: &str, column: &ColumnDef) -> Vec<String>;

    /// `ALTER TABLE ... DROP COLUMN` statement(s).
    fn build_drop_column(&self, table: &str, column_name: &str) -> Vec<String>;

    /// `ALTER TABLE ... RENAME COLUMN` statement(s).
    fn build_rename_column(&self, table: &str, old_name: &str, new_name: &str) -> Vec<String>;

    /// Column type / nullability change statement(s). May return several
    /// statements when the dialect splits type and nullability changes.
    fn build_modify_column(&self, table: &str, column: &ColumnDef) -> Vec<String>;
}

/// Async schema discovery (returned as Pin<Box<dyn Future>> for dyn compatibility).
pub trait SchemaDiscovery: Send + Sync {
    fn discover_columns(&self, database: &str, table: &str) -> DiscoverColumnsFuture<'_>;
}

/// Boxed future returned by [`SchemaDiscovery::discover_columns`].
pub type DiscoverColumnsFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Vec<ColumnDef>, Box<dyn std::error::Error + Send + Sync>>> + 'a>,
>;

pub struct MySqlDialect;
pub struct PostgresDialect;
pub struct TiDbDialect;

/// Quote a possibly dotted table name per-dialect (`` `db`.`t` `` /
/// `"db"."t"`); quoting the whole dotted string would create a table whose
/// name contains a literal dot.
fn quote_dotted_backtick(table: &str) -> String {
    table
        .split('.')
        .map(|part| format!("`{}`", part))
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_dotted_double(table: &str) -> String {
    table
        .split('.')
        .map(|part| format!("\"{}\"", part))
        .collect::<Vec<_>>()
        .join(".")
}

fn decimal_type(precision: u8, scale: i8) -> ColumnType {
    // Clamp to MySQL/Postgres limits: precision 1..=65 (MySQL) / 1..=1000 (PG),
    // scale 0..=30 (MySQL). Use the stricter bound so one DDL fits both.
    ColumnType::Decimal {
        precision: precision.clamp(1, 65),
        scale: scale.clamp(0, 30),
    }
}

impl MySqlDialect {
    fn sql_type(column: &ColumnDef) -> String {
        match &column.column_type {
            ColumnType::Bool => "TINYINT(1)".to_string(),
            ColumnType::Int8 => "TINYINT".to_string(),
            ColumnType::Int16 => "SMALLINT".to_string(),
            ColumnType::Int32 => "INT".to_string(),
            ColumnType::Int64 => "BIGINT".to_string(),
            ColumnType::UInt8 => "TINYINT UNSIGNED".to_string(),
            ColumnType::UInt16 => "SMALLINT UNSIGNED".to_string(),
            ColumnType::UInt32 => "INT UNSIGNED".to_string(),
            ColumnType::UInt64 => "BIGINT UNSIGNED".to_string(),
            ColumnType::Float32 => "FLOAT".to_string(),
            ColumnType::Float64 => "DOUBLE".to_string(),
            ColumnType::Decimal { precision, scale } => {
                format!(
                    "DECIMAL({},{})",
                    (*precision).clamp(1, 65),
                    (*scale).clamp(0, 30)
                )
            }
            // Indexed/PK columns cannot be TEXT in MySQL; use VARCHAR.
            ColumnType::String => {
                if column.primary_key {
                    "VARCHAR(255)".to_string()
                } else {
                    "TEXT".to_string()
                }
            }
            ColumnType::Bytes => "BLOB".to_string(),
            ColumnType::Json => "JSON".to_string(),
            ColumnType::Date => "DATE".to_string(),
            ColumnType::Time => "TIME".to_string(),
            ColumnType::DateTime => "DATETIME".to_string(),
            ColumnType::TimestampTz => "TIMESTAMP".to_string(),
            // Durations are nanosecond counts.
            ColumnType::Duration => "BIGINT".to_string(),
            ColumnType::Array { .. } | ColumnType::Map { .. } => "JSON".to_string(),
            ColumnType::Nullable(inner) => {
                let mut col = column.clone();
                col.column_type = (**inner).clone();
                Self::sql_type(&col)
            }
        }
    }

    fn column_def_sql(column: &ColumnDef) -> String {
        let mut sql = format!("`{}` {}", column.name, Self::sql_type(column));
        if !column.nullable {
            sql.push_str(" NOT NULL");
        }
        sql
    }
}

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
            "decimal" | "numeric" => decimal_type(
                length.unwrap_or(10).min(u32::from(u8::MAX)) as u8,
                scale.unwrap_or(0),
            ),
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
                let mut col = c.clone();
                col.primary_key = col.primary_key || schema.primary_key.contains(&col.name);
                if col.primary_key {
                    col.nullable = false;
                }
                Self::column_def_sql(&col)
            })
            .collect();
        let qualified = schema
            .table_identifier
            .split('.')
            .map(|part| format!("`{}`", part))
            .collect::<Vec<_>>()
            .join(".");
        let mut sql = format!("CREATE TABLE {} (", qualified);
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
    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name)
    }
    fn sql_type_for(&self, column: &ColumnDef) -> String {
        Self::sql_type(column)
    }
    fn build_add_column(&self, table: &str, column: &ColumnDef) -> Vec<String> {
        vec![format!(
            "ALTER TABLE {} ADD COLUMN {}",
            quote_dotted_backtick(table),
            Self::column_def_sql(column)
        )]
    }
    fn build_drop_column(&self, table: &str, column_name: &str) -> Vec<String> {
        vec![format!(
            "ALTER TABLE {} DROP COLUMN `{}`",
            quote_dotted_backtick(table),
            column_name
        )]
    }
    fn build_rename_column(&self, table: &str, old_name: &str, new_name: &str) -> Vec<String> {
        // MySQL 8.0+ / TiDB support RENAME COLUMN; older servers would need
        // CHANGE COLUMN with the full definition.
        vec![format!(
            "ALTER TABLE {} RENAME COLUMN `{}` TO `{}`",
            quote_dotted_backtick(table),
            old_name,
            new_name
        )]
    }
    fn build_modify_column(&self, table: &str, column: &ColumnDef) -> Vec<String> {
        vec![format!(
            "ALTER TABLE {} MODIFY COLUMN {}",
            quote_dotted_backtick(table),
            Self::column_def_sql(column)
        )]
    }
}

impl SchemaDiscovery for MySqlDialect {
    fn discover_columns(&self, _database: &str, _table: &str) -> DiscoverColumnsFuture<'_> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl PostgresDialect {
    fn sql_type(column: &ColumnDef) -> String {
        match &column.column_type {
            ColumnType::Bool => "BOOLEAN".to_string(),
            ColumnType::Int8 | ColumnType::Int16 => "SMALLINT".to_string(),
            ColumnType::Int32 => "INTEGER".to_string(),
            ColumnType::Int64 => "BIGINT".to_string(),
            // Postgres has no unsigned integers.
            ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 => {
                "NUMERIC(20,0)".to_string()
            }
            ColumnType::Float32 => "REAL".to_string(),
            ColumnType::Float64 => "DOUBLE PRECISION".to_string(),
            ColumnType::Decimal { precision, scale } => {
                format!(
                    "NUMERIC({},{})",
                    (*precision).clamp(1, 255),
                    (*scale).clamp(0, 127)
                )
            }
            ColumnType::String => "TEXT".to_string(),
            ColumnType::Bytes => "BYTEA".to_string(),
            ColumnType::Json => "JSONB".to_string(),
            ColumnType::Date => "DATE".to_string(),
            ColumnType::Time => "TIME".to_string(),
            ColumnType::DateTime => "TIMESTAMP".to_string(),
            ColumnType::TimestampTz => "TIMESTAMPTZ".to_string(),
            ColumnType::Duration => "BIGINT".to_string(),
            ColumnType::Array { element_type } => {
                let elem_col = ColumnDef::new("elem", (**element_type).clone());
                format!("{}[]", Self::sql_type(&elem_col))
            }
            ColumnType::Map { .. } => "JSONB".to_string(),
            ColumnType::Nullable(inner) => {
                let mut col = column.clone();
                col.column_type = (**inner).clone();
                Self::sql_type(&col)
            }
        }
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
            "numeric" | "decimal" => decimal_type(length.unwrap_or(10) as u8, scale.unwrap_or(0)),
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
                let mut col = c.clone();
                col.primary_key = col.primary_key || schema.primary_key.contains(&col.name);
                if col.primary_key {
                    col.nullable = false;
                }
                let mut s = format!("\"{}\" {}", c.name, Self::sql_type(&col));
                if !col.nullable {
                    s.push_str(" NOT NULL");
                }
                s
            })
            .collect();
        let qualified = schema
            .table_identifier
            .split('.')
            .map(|part| format!("\"{}\"", part))
            .collect::<Vec<_>>()
            .join(".");
        let mut sql = format!("CREATE TABLE {} (", qualified);
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
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }
    fn sql_type_for(&self, column: &ColumnDef) -> String {
        Self::sql_type(column)
    }
    fn build_add_column(&self, table: &str, column: &ColumnDef) -> Vec<String> {
        let mut sql = format!(
            "ALTER TABLE {} ADD COLUMN \"{}\" {}",
            quote_dotted_double(table),
            column.name,
            Self::sql_type(column)
        );
        if !column.nullable {
            sql.push_str(" NOT NULL");
        }
        vec![sql]
    }
    fn build_drop_column(&self, table: &str, column_name: &str) -> Vec<String> {
        vec![format!(
            "ALTER TABLE {} DROP COLUMN \"{}\"",
            quote_dotted_double(table),
            column_name
        )]
    }
    fn build_rename_column(&self, table: &str, old_name: &str, new_name: &str) -> Vec<String> {
        vec![format!(
            "ALTER TABLE {} RENAME COLUMN \"{}\" TO \"{}\"",
            quote_dotted_double(table),
            old_name,
            new_name
        )]
    }
    fn build_modify_column(&self, table: &str, column: &ColumnDef) -> Vec<String> {
        let mut stmts = vec![format!(
            "ALTER TABLE {} ALTER COLUMN \"{}\" TYPE {}",
            quote_dotted_double(table),
            column.name,
            Self::sql_type(column)
        )];
        // Nullability needs a separate statement in Postgres.
        if column.nullable {
            stmts.push(format!(
                "ALTER TABLE {} ALTER COLUMN \"{}\" DROP NOT NULL",
                quote_dotted_double(table),
                column.name
            ));
        } else {
            stmts.push(format!(
                "ALTER TABLE {} ALTER COLUMN \"{}\" SET NOT NULL",
                quote_dotted_double(table),
                column.name
            ));
        }
        stmts
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
    fn quote_identifier(&self, name: &str) -> String {
        MySqlDialect.quote_identifier(name)
    }
    fn sql_type_for(&self, column: &ColumnDef) -> String {
        MySqlDialect.sql_type_for(column)
    }
    fn build_add_column(&self, table: &str, column: &ColumnDef) -> Vec<String> {
        MySqlDialect.build_add_column(table, column)
    }
    fn build_drop_column(&self, table: &str, column_name: &str) -> Vec<String> {
        MySqlDialect.build_drop_column(table, column_name)
    }
    fn build_rename_column(&self, table: &str, old_name: &str, new_name: &str) -> Vec<String> {
        MySqlDialect.build_rename_column(table, old_name, new_name)
    }
    fn build_modify_column(&self, table: &str, column: &ColumnDef) -> Vec<String> {
        MySqlDialect.build_modify_column(table, column)
    }
}

impl SchemaDiscovery for TiDbDialect {
    fn discover_columns(&self, database: &str, table: &str) -> DiscoverColumnsFuture<'_> {
        MySqlDialect.discover_columns(database, table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk_col(name: &str) -> ColumnDef {
        ColumnDef::new(name, ColumnType::Int64).primary_key()
    }

    #[test]
    fn test_mysql_create_and_alter() {
        let schema = TableSchema::new(
            "db.users",
            vec![pk_col("id"), ColumnDef::new("name", ColumnType::String)],
        );
        let sql = MySqlDialect.build_create_table(&schema);
        assert!(sql.contains("`id` BIGINT NOT NULL"));
        assert!(sql.contains("`name` TEXT"));
        assert!(sql.contains("PRIMARY KEY (`id`)"));

        let add = MySqlDialect.build_add_column(
            "users",
            &ColumnDef::new("score", ColumnType::Int32).not_null(),
        );
        assert_eq!(
            add,
            vec!["ALTER TABLE `users` ADD COLUMN `score` INT NOT NULL".to_string()]
        );

        let modify = MySqlDialect.build_modify_column(
            "users",
            &ColumnDef::new("score", ColumnType::Float64).not_null(),
        );
        assert_eq!(
            modify,
            vec!["ALTER TABLE `users` MODIFY COLUMN `score` DOUBLE NOT NULL".to_string()]
        );

        let rename = MySqlDialect.build_rename_column("users", "score", "rating");
        assert_eq!(
            rename,
            vec!["ALTER TABLE `users` RENAME COLUMN `score` TO `rating`".to_string()]
        );

        let drop = MySqlDialect.build_drop_column("users", "rating");
        assert_eq!(
            drop,
            vec!["ALTER TABLE `users` DROP COLUMN `rating`".to_string()]
        );
    }

    #[test]
    fn test_mysql_pk_string_column_not_text() {
        let col = ColumnDef::new("code", ColumnType::String).primary_key();
        assert_eq!(MySqlDialect.sql_type_for(&col), "VARCHAR(255)");
        let plain = ColumnDef::new("code", ColumnType::String);
        assert_eq!(MySqlDialect.sql_type_for(&plain), "TEXT");
    }

    #[test]
    fn test_postgres_alter_statements() {
        let modify = PostgresDialect.build_modify_column(
            "users",
            &ColumnDef::new(
                "score",
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
        );
        assert_eq!(
            modify,
            vec![
                "ALTER TABLE \"users\" ALTER COLUMN \"score\" TYPE NUMERIC(10,2)".to_string(),
                "ALTER TABLE \"users\" ALTER COLUMN \"score\" DROP NOT NULL".to_string(),
            ]
        );

        let add = PostgresDialect.build_add_column(
            "users",
            &ColumnDef::new(
                "tags",
                ColumnType::Array {
                    element_type: Box::new(ColumnType::String),
                },
            ),
        );
        assert_eq!(
            add,
            vec!["ALTER TABLE \"users\" ADD COLUMN \"tags\" TEXT[]".to_string()]
        );
    }

    #[test]
    fn test_dotted_table_identifiers_are_split() {
        let schema = TableSchema::new(
            "db.users",
            vec![ColumnDef::new("id", ColumnType::Int64).primary_key()],
        );
        let sql = MySqlDialect.build_create_table(&schema);
        assert!(sql.starts_with("CREATE TABLE `db`.`users` ("));

        let alter =
            MySqlDialect.build_add_column("db.users", &ColumnDef::new("c", ColumnType::String));
        assert_eq!(
            alter,
            vec!["ALTER TABLE `db`.`users` ADD COLUMN `c` TEXT".to_string()]
        );

        let pg_alter = PostgresDialect.build_drop_column("public.users", "c");
        assert_eq!(
            pg_alter,
            vec!["ALTER TABLE \"public\".\"users\" DROP COLUMN \"c\"".to_string()]
        );
    }

    #[test]
    fn test_tidb_delegates_to_mysql() {
        assert_eq!(
            TiDbDialect.build_add_column("t", &ColumnDef::new("c", ColumnType::Bool)),
            MySqlDialect.build_add_column("t", &ColumnDef::new("c", ColumnType::Bool))
        );
        assert_eq!(TiDbDialect.dialect_name(), "tidb");
    }
}
