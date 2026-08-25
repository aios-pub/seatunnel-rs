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

//! Common connector base utilities.

use std::collections::HashMap;
use uuid::Uuid;

use seatunnel_api::{row::Row, source::source_split::SourceSplit, schema::TableSchema};

/// Configuration for any connector with typed accessors.
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    props: HashMap<String, String>,
}

impl ConnectorConfig {
    pub fn new(props: HashMap<String, String>) -> Self {
        ConnectorConfig { props }
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.props.get(key)
    }
    pub fn get_string(&self, key: &str, default: &str) -> String {
        self.props.get(key).map(|v| v.clone()).unwrap_or(default.to_string())
    }
    pub fn get_int(&self, key: &str, default: i64) -> i64 {
        self.props.get(key).and_then(|v| v.parse::<i64>().ok()).unwrap_or(default)
    }
    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.props.get(key).and_then(|v| v.parse::<bool>().ok()).unwrap_or(default)
    }
    pub fn to_hashmap(&self) -> HashMap<String, String> {
        self.props.clone()
    }
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        ConnectorConfig { props: HashMap::new() }
    }
}

/// A checkpointable source split with metadata.
#[derive(Debug, Clone)]
pub struct BaseSourceSplit {
    pub id: String,
    pub kind: String,
    pub metadata: HashMap<String, String>,
}

impl BaseSourceSplit {
    pub fn new(kind: &str) -> Self {
        BaseSourceSplit {
            id: format!("{}-{}", kind, Uuid::new_v4()),
            kind: kind.to_string(),
            metadata: HashMap::new(),
        }
    }
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

impl SourceSplit for BaseSourceSplit {
    fn split_id(&self) -> &str {
        &self.id
    }
}

/// Create a row from a map of field values keyed by column name.
pub fn row_from_map(schema: &TableSchema, data: &HashMap<String, seatunnel_api::Field>) -> Row {
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        row.set(i, data.get(&col.name).cloned().unwrap_or(seatunnel_api::Field::Null));
    }
    row
}
