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

//! JDBC URL parsing: `jdbc:mysql://host:port/db?params`,
//! `jdbc:postgresql://host:port/db?params` and bare `mysql://` /
//! `postgres://` forms.

use crate::dialect::JdbcDialectKind;

/// Parsed connection endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JdbcUrl {
    pub dialect: JdbcDialectKind,
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl JdbcDialectKind {
    pub fn default_port(&self) -> u16 {
        match self {
            JdbcDialectKind::MySql | JdbcDialectKind::TiDB => 3306,
            JdbcDialectKind::Postgres => 5432,
        }
    }
}

/// Detect the dialect family from a JDBC URL.
pub fn detect_dialect(url: &str) -> JdbcDialectKind {
    let lower = url.to_lowercase();
    if lower.contains("postgres") {
        JdbcDialectKind::Postgres
    } else {
        // mysql, mariadb and tidb all speak the MySQL wire protocol
        JdbcDialectKind::MySql
    }
}

/// Parse a JDBC-style URL into host/port/database.
pub fn parse_jdbc_url(url: &str) -> anyhow::Result<JdbcUrl> {
    let dialect = detect_dialect(url);
    let default_port = dialect.default_port();

    let without_scheme = url
        .strip_prefix("jdbc:")
        .unwrap_or(url)
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url);

    let (authority, rest) = without_scheme.split_once('/').unwrap_or((without_scheme, ""));
    let database = rest.split('?').next().unwrap_or("").to_string();

    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h.trim_start_matches('[').trim_end_matches(']').to_string(),
            p.parse::<u16>().unwrap_or(default_port),
        ),
        None => (authority.to_string(), default_port),
    };

    if host.is_empty() {
        anyhow::bail!("invalid JDBC url (no host): {}", url);
    }

    Ok(JdbcUrl {
        dialect,
        host,
        port,
        database,
    })
}

/// Split a possibly qualified table name (`db.users` / `public.users` /
/// plain `users`) into (schema-or-database, table).
pub fn split_table_name(table: &str) -> (Option<String>, String) {
    match table.split_once('.') {
        Some((ns, t)) => (Some(ns.to_string()), t.to_string()),
        None => (None, table.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_dialect() {
        assert_eq!(detect_dialect("jdbc:mysql://h:3306/db"), JdbcDialectKind::MySql);
        assert_eq!(detect_dialect("jdbc:mariadb://h:3306/db"), JdbcDialectKind::MySql);
        assert_eq!(detect_dialect("jdbc:postgresql://h:5432/db"), JdbcDialectKind::Postgres);
        assert_eq!(detect_dialect("postgres://h/db"), JdbcDialectKind::Postgres);
    }

    #[test]
    fn test_parse_mysql_url() {
        let parsed = parse_jdbc_url("jdbc:mysql://10.10.100.88:4001/ailearn_yace").unwrap();
        assert_eq!(parsed.host, "10.10.100.88");
        assert_eq!(parsed.port, 4001);
        assert_eq!(parsed.database, "ailearn_yace");
    }

    #[test]
    fn test_parse_default_port_and_params() {
        let parsed = parse_jdbc_url("jdbc:mysql://localhost/mydb?useSSL=false").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 3306);
        assert_eq!(parsed.database, "mydb");
    }

    #[test]
    fn test_parse_postgres_url() {
        let parsed = parse_jdbc_url("jdbc:postgresql://127.0.0.1:5432/postgres").unwrap();
        assert_eq!(parsed.dialect, JdbcDialectKind::Postgres);
        assert_eq!(parsed.port, 5432);
        assert_eq!(parsed.database, "postgres");
    }

    #[test]
    fn test_split_table_name() {
        assert_eq!(split_table_name("db.users"), (Some("db".into()), "users".into()));
        assert_eq!(split_table_name("public.users"), (Some("public".into()), "users".into()));
        assert_eq!(split_table_name("users"), (None, "users".into()));
    }
}
