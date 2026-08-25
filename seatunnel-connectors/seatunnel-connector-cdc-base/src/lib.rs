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

//! CDC (Change Data Capture) base framework.
//!
//! Provides the shared foundation for all CDC connectors:
//! - Snapshot + Incremental hybrid split model
//! - Watermark-based exactly-once deduplication
//! - Schema change event handling
//! - Common offsets and state types

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use seatunnel_api::{
    row::{Row, RowKind},
    schema::TableSchema,
    source::source_split::SourceSplit,
};

/// The two phases of a CDC source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcPhase {
    Snapshot,
    Incremental,
}

impl fmt::Display for CdcPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CdcPhase::Snapshot => write!(f, "SNAPSHOT"),
            CdcPhase::Incremental => write!(f, "INCREMENTAL"),
        }
    }
}

/// A watermark value used to track exactly-once processing boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Watermark {
    Min,
    Max,
    Value(i64),
}

impl PartialOrd for Watermark {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Watermark {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Watermark::Min, Watermark::Min) => std::cmp::Ordering::Equal,
            (Watermark::Min, _) => std::cmp::Ordering::Less,
            (_, Watermark::Min) => std::cmp::Ordering::Greater,
            (Watermark::Max, Watermark::Max) => std::cmp::Ordering::Equal,
            (Watermark::Max, _) => std::cmp::Ordering::Greater,
            (_, Watermark::Max) => std::cmp::Ordering::Less,
            (Watermark::Value(a), Watermark::Value(b)) => a.cmp(b),
        }
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Watermark::Min
    }
}

impl Watermark {
    pub fn is_min(&self) -> bool {
        matches!(self, Watermark::Min)
    }

    pub fn is_max(&self) -> bool {
        matches!(self, Watermark::Max)
    }
}

/// Snapshot phase split. Contains table name and key range.
#[derive(Debug, Clone)]
pub struct SnapshotSplit {
    pub id: String,
    pub database: String,
    pub table: String,
    pub split_column: String,
    pub start_key: String,
    pub end_key: String,
    pub low_watermark: Watermark,
    pub high_watermark: Watermark,
}

impl SnapshotSplit {
    pub fn new(database: &str, table: &str, split_column: &str, start: &str, end: &str) -> Self {
        SnapshotSplit {
            id: format!("snapshot-{}-{}-{}", database, table, uuid::Uuid::new_v4()),
            database: database.to_string(),
            table: table.to_string(),
            split_column: split_column.to_string(),
            start_key: start.to_string(),
            end_key: end.to_string(),
            low_watermark: Watermark::Min,
            high_watermark: Watermark::Max,
        }
    }
}

impl SourceSplit for SnapshotSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Incremental phase split. Contains the replication offset.
#[derive(Debug, Clone)]
pub struct IncrementalSplit {
    pub id: String,
    pub database: String,
    pub table: String,
    pub offset: HashMap<String, String>,
}

impl IncrementalSplit {
    pub fn new(database: &str, table: &str) -> Self {
        IncrementalSplit {
            id: format!("incremental-{}-{}-{}", database, table, uuid::Uuid::new_v4()),
            database: database.to_string(),
            table: table.to_string(),
            offset: HashMap::new(),
        }
    }

    pub fn with_offset(mut self, key: &str, value: &str) -> Self {
        self.offset.insert(key.to_string(), value.to_string());
        self
    }
}

impl SourceSplit for IncrementalSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Checkpoint state for CDC sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcState {
    pub phase: CdcPhase,
    pub watermark: Watermark,
    pub offset: HashMap<String, String>,
}

impl Default for CdcState {
    fn default() -> Self {
        CdcState {
            phase: CdcPhase::Snapshot,
            watermark: Watermark::Min,
            offset: HashMap::new(),
        }
    }
}

impl CdcState {
    pub fn new(phase: CdcPhase, offset: HashMap<String, String>) -> Self {
        CdcState {
            phase,
            watermark: Watermark::Min,
            offset,
        }
    }

    pub fn with_watermark(mut self, watermark: Watermark) -> Self {
        self.watermark = watermark;
        self
    }
}

/// Schema change event for CDC.
#[derive(Debug, Clone)]
pub enum SchemaChangeEvent {
    AddColumn { table: String, column: seatunnel_api::ColumnDef },
    DropColumn { table: String, column_name: String },
    RenameColumn { table: String, old_name: String, new_name: String },
    AlterType { table: String, column_name: String, new_type: seatunnel_api::ColumnType },
}

/// Common CDC configuration.
#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub table_name: String,
    pub startup_mode: String,
}

impl CdcConfig {
    pub fn new(hostname: &str, port: u16, username: &str, password: &str, database: &str, table: &str) -> Self {
        CdcConfig {
            hostname: hostname.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            table_name: table.to_string(),
            startup_mode: "initial".to_string(),
        }
    }
}

/// Marker trait for CDC connectors.
pub trait CdcSource {
    fn config(&self) -> &CdcConfig;
    fn schema(&self) -> Option<&TableSchema> { None }
}

/// Watermark buffer for exactly-once deduplication.
#[derive(Debug, Clone)]
pub struct WatermarkBuffer {
    low_watermark: Watermark,
    high_watermark: Watermark,
}

impl WatermarkBuffer {
    pub fn new() -> Self {
        WatermarkBuffer {
            low_watermark: Watermark::Min,
            high_watermark: Watermark::Max,
        }
    }

    pub fn advance_low_watermark(&mut self, watermark: Watermark) {
        if watermark > self.low_watermark {
            self.low_watermark = watermark;
        }
    }

    pub fn advance_high_watermark(&mut self, watermark: Watermark) {
        if watermark < self.high_watermark {
            self.high_watermark = watermark;
        }
    }

    pub fn should_emit(&self, event_watermark: &Watermark) -> bool {
        !event_watermark.is_min() && event_watermark < &self.low_watermark
    }

    pub fn low_watermark(&self) -> &Watermark {
        &self.low_watermark
    }

    pub fn high_watermark(&self) -> &Watermark {
        &self.high_watermark
    }
}

impl Default for WatermarkBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_split() {
        let split = SnapshotSplit::new("mydb", "users", "id", "0", "100");
        assert!(split.split_id().starts_with("snapshot-"));
        assert_eq!(split.database, "mydb");
        assert_eq!(split.table, "users");
        assert!(split.low_watermark.is_min());
        assert!(split.high_watermark.is_max());
    }

    #[test]
    fn test_incremental_split() {
        let split = IncrementalSplit::new("mydb", "users").with_offset("file", "binlog.000001").with_offset("pos", "12345");
        assert!(split.split_id().starts_with("incremental-"));
        assert_eq!(split.offset.get("file"), Some(&"binlog.000001".to_string()));
    }

    #[test]
    fn test_cdc_state() {
        let state = CdcState::new(CdcPhase::Incremental, HashMap::new()).with_watermark(Watermark::Value(42));
        assert_eq!(state.phase, CdcPhase::Incremental);
        assert_eq!(state.watermark, Watermark::Value(42));
    }

    #[test]
    fn test_watermark_buffer() {
        let mut buf = WatermarkBuffer::new();
        buf.advance_high_watermark(Watermark::Value(100));
        assert_eq!(*buf.high_watermark(), Watermark::Value(100));
        buf.advance_low_watermark(Watermark::Value(50));
        assert_eq!(*buf.low_watermark(), Watermark::Value(50));
        assert!(buf.should_emit(&Watermark::Value(49)));
        assert!(!buf.should_emit(&Watermark::Value(51)));
        assert!(!buf.should_emit(&Watermark::Min));
    }

    #[test]
    fn test_schema_change_event() {
        let event = SchemaChangeEvent::AddColumn {
            table: "users".to_string(),
            column: seatunnel_api::ColumnDef::new("email".to_string(), seatunnel_api::ColumnType::String),
        };
        match event {
            SchemaChangeEvent::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "email");
            }
            _ => panic!("wrong variant"),
        }
    }
}
