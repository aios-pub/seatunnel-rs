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

//! SQL generation for the JDBC connector, mirroring the Java connector's
//! `JdbcDialect` per-database statement builders (identifier quoting,
//! INSERT / upsert / delete statements, placeholder style).

use seatunnel_api::ColumnType;
use seatunnel_api::schema::{
    ColumnDef, DatabaseDialect, MySqlDialect, PostgresDialect, TableSchema,
};

/// Database family this connector speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JdbcDialectKind {
    MySql,
    Postgres,
    /// TiDB speaks the MySQL wire protocol and SQL dialect.
    TiDB,
}

impl JdbcDialectKind {
    pub fn api_dialect(&self) -> &'static dyn DatabaseDialect {
        match self {
            JdbcDialectKind::MySql | JdbcDialectKind::TiDB => &MySqlDialect,
            JdbcDialectKind::Postgres => &PostgresDialect,
        }
    }

    pub fn quote(&self, ident: &str) -> String {
        self.api_dialect().quote_identifier(ident)
    }

    /// Quote a possibly dotted table name (`db.users` → `` `db`.`users` ``).
    pub fn quote_table(&self, table: &str) -> String {
        table
            .split('.')
            .map(|part| self.quote(part))
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Placeholder for the `ordinal`-th (1-based) parameter, optionally
    /// cast to the target column type. Postgres binds most values as text
    /// so every placeholder needs an explicit cast; MySQL uses `?`.
    pub fn placeholder(&self, ordinal: usize, column_type: &ColumnType) -> String {
        match self {
            JdbcDialectKind::MySql | JdbcDialectKind::TiDB => "?".to_string(),
            JdbcDialectKind::Postgres => {
                let cast = self.pg_cast(column_type);
                match cast {
                    Some(t) => format!("${}::{}", ordinal, t),
                    None => format!("${}", ordinal),
                }
            }
        }
    }

    /// Explicit cast suffix for a Postgres text-typed parameter; `None`
    /// means the parameter binds natively (bool / bytea / text).
    fn pg_cast(&self, column_type: &ColumnType) -> Option<&'static str> {
        match column_type {
            ColumnType::Bool | ColumnType::String | ColumnType::Bytes => None,
            ColumnType::Int8 | ColumnType::Int16 => Some("smallint"),
            ColumnType::Int32 => Some("int"),
            ColumnType::Int64 => Some("bigint"),
            ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 => {
                Some("numeric")
            }
            ColumnType::Float32 => Some("real"),
            ColumnType::Float64 => Some("double precision"),
            ColumnType::Decimal { .. } => Some("numeric"),
            ColumnType::Json => Some("jsonb"),
            ColumnType::Date => Some("date"),
            ColumnType::Time => Some("time"),
            ColumnType::DateTime => Some("timestamp"),
            ColumnType::TimestampTz => Some("timestamptz"),
            ColumnType::Duration => Some("bigint"),
            ColumnType::Array { .. } | ColumnType::Map { .. } => Some("jsonb"),
            ColumnType::Nullable(inner) => self.pg_cast(inner),
        }
    }

    /// Plain multi-row INSERT statement.
    pub fn build_insert(
        &self,
        table: &str,
        columns: &[String],
        column_types: &[ColumnType],
        row_count: usize,
    ) -> String {
        let cols: Vec<String> = columns.iter().map(|c| self.quote(c)).collect();
        let one_row = {
            let placeholders: Vec<String> = column_types
                .iter()
                .enumerate()
                .map(|(i, ct)| self.placeholder(i + 1, ct))
                .collect();
            format!("({})", placeholders.join(", "))
        };
        let rows: Vec<String> = std::iter::repeat_n(one_row, row_count).collect();
        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(table),
            cols.join(", "),
            rows.join(", ")
        )
    }

    /// Native upsert statement for a batch of rows:
    /// MySQL `INSERT ... ON DUPLICATE KEY UPDATE`,
    /// Postgres `INSERT ... ON CONFLICT (pk) DO UPDATE SET`.
    pub fn build_upsert(
        &self,
        table: &str,
        columns: &[String],
        column_types: &[ColumnType],
        primary_keys: &[String],
        row_count: usize,
    ) -> String {
        let mut sql = self.build_insert(table, columns, column_types, row_count);
        match self {
            JdbcDialectKind::MySql | JdbcDialectKind::TiDB => {
                let updates: Vec<String> = columns
                    .iter()
                    .filter(|c| !primary_keys.contains(c))
                    .map(|c| format!("{} = VALUES({})", self.quote(c), self.quote(c)))
                    .collect();
                if !updates.is_empty() {
                    sql.push_str(" ON DUPLICATE KEY UPDATE ");
                    sql.push_str(&updates.join(", "));
                }
            }
            JdbcDialectKind::Postgres => {
                let conflict_cols: Vec<String> =
                    primary_keys.iter().map(|c| self.quote(c)).collect();
                let updates: Vec<String> = columns
                    .iter()
                    .filter(|c| !primary_keys.contains(c))
                    .map(|c| format!("{} = EXCLUDED.{}", self.quote(c), self.quote(c)))
                    .collect();
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    conflict_cols.join(", "),
                    if updates.is_empty() {
                        "NOTHING".to_string()
                    } else {
                        updates.join(", ")
                    }
                ));
            }
        }
        sql
    }

    /// DELETE statement by primary key (single row).
    pub fn build_delete_by_pk(
        &self,
        table: &str,
        primary_keys: &[String],
        key_types: &[ColumnType],
    ) -> String {
        let where_parts: Vec<String> = primary_keys
            .iter()
            .zip(key_types)
            .enumerate()
            .map(|(i, (pk, ct))| format!("{} = {}", self.quote(pk), self.placeholder(i + 1, ct)))
            .collect();
        format!(
            "DELETE FROM {} WHERE {}",
            self.quote_table(table),
            where_parts.join(" AND ")
        )
    }

    /// Build DDL statements for a schema change using the api dialects.
    pub fn build_schema_change(
        &self,
        table: &str,
        change: &seatunnel_api::SchemaChange,
    ) -> Vec<String> {
        use seatunnel_api::SchemaChange;
        match change {
            SchemaChange::AddColumn { column, .. } => {
                self.api_dialect().build_add_column(table, column)
            }
            SchemaChange::DropColumn { column_name, .. } => {
                self.api_dialect().build_drop_column(table, column_name)
            }
            SchemaChange::RenameColumn {
                old_name, new_name, ..
            } => self
                .api_dialect()
                .build_rename_column(table, old_name, new_name),
            SchemaChange::ModifyColumn { column, .. } => {
                self.api_dialect().build_modify_column(table, column)
            }
        }
    }

    /// CREATE TABLE statement for a schema.
    pub fn build_create_table(&self, schema: &TableSchema) -> String {
        self.api_dialect().build_create_table(schema)
    }

    /// SQL type for a column (used by auto-create and row inference).
    pub fn sql_type_for(&self, column: &ColumnDef) -> String {
        self.api_dialect().sql_type_for(column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_insert_upsert_delete() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let types = vec![ColumnType::Int64, ColumnType::String];
        let pks = vec!["id".to_string()];

        let insert = JdbcDialectKind::MySql.build_insert("db.users", &cols, &types, 2);
        assert_eq!(
            insert,
            "INSERT INTO `db`.`users` (`id`, `name`) VALUES (?, ?), (?, ?)"
        );

        let upsert = JdbcDialectKind::MySql.build_upsert("users", &cols, &types, &pks, 1);
        assert_eq!(
            upsert,
            "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );

        let delete = JdbcDialectKind::MySql.build_delete_by_pk("users", &pks, &[ColumnType::Int64]);
        assert_eq!(delete, "DELETE FROM `users` WHERE `id` = ?");
    }

    #[test]
    fn test_postgres_insert_upsert_delete_with_casts() {
        let cols = vec!["id".to_string(), "score".to_string(), "born".to_string()];
        let types = vec![
            ColumnType::Int64,
            ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            ColumnType::Date,
        ];
        let pks = vec!["id".to_string()];

        let upsert = JdbcDialectKind::Postgres.build_upsert("public.users", &cols, &types, &pks, 1);
        assert_eq!(
            upsert,
            "INSERT INTO \"public\".\"users\" (\"id\", \"score\", \"born\") VALUES ($1::bigint, $2::numeric, $3::date) ON CONFLICT (\"id\") DO UPDATE SET \"score\" = EXCLUDED.\"score\", \"born\" = EXCLUDED.\"born\""
        );

        let delete =
            JdbcDialectKind::Postgres.build_delete_by_pk("users", &pks, &[ColumnType::Int64]);
        assert_eq!(delete, "DELETE FROM \"users\" WHERE \"id\" = $1::bigint");
    }

    #[test]
    fn test_tidb_matches_mysql() {
        let cols = vec!["a".to_string()];
        let types = vec![ColumnType::String];
        assert_eq!(
            JdbcDialectKind::TiDB.build_insert("t", &cols, &types, 1),
            JdbcDialectKind::MySql.build_insert("t", &cols, &types, 1)
        );
    }
}
