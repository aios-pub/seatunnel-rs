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

//! JDBC Connector for MySQL, TiDB (MySQL wire) and PostgreSQL.
//!
//! - Source: bounded snapshot reads with parallel keyset splits,
//!   schema discovery, custom query support.
//! - Sink: batched insert / native upsert (`ON DUPLICATE KEY UPDATE`,
//!   `ON CONFLICT DO UPDATE`) / delete, save modes (auto table create,
//!   truncate, custom SQL) and mid-stream schema evolution via
//!   `ALTER TABLE`.
//!
//! Layout:
//! - [`url`] — JDBC URL parsing
//! - [`dialect`] — per-database SQL generation
//! - [`conn`] — unified pooled async endpoint (mysql_async / tokio-postgres)
//! - [`value`] — Field ↔ driver value conversions
//! - [`catalog`] — information_schema / pg_catalog discovery
//! - [`source`] / [`sink`] — reader & writer
//! - [`xa_sink`] — MySQL XA exactly-once writer & committer

pub mod catalog;
pub mod conn;
pub mod dialect;
pub mod sink;
pub mod source;
pub mod url;
pub mod value;
pub mod xa_sink;

pub use conn::{DbEndpoint, QueryResult};
pub use dialect::JdbcDialectKind;
pub use sink::{DataSaveMode, JdbcSink, JdbcSinkConfig, JdbcSinkWriter, SchemaSaveMode};
pub use source::{JdbcSourceConfig, JdbcSourceReader, JdbcSourceState};
pub use url::{JdbcUrl, parse_jdbc_url};
pub use value::SqlValue;

pub use source::JdbcSplit;

pub use xa_sink::{XaCommitInfo, XaSink, XaSinkCommitter, XaSinkConfig, XaSinkWriter};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_reexports() {
        let parsed = parse_jdbc_url("jdbc:postgresql://127.0.0.1:5432/app").unwrap();
        assert_eq!(parsed.dialect, JdbcDialectKind::Postgres);
        assert_eq!(parsed.database, "app");
    }
}
