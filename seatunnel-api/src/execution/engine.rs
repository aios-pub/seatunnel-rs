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

use super::execution_mode::ExecutionMode;
use crate::row::{Field, Row, RowKind};

/// The SeaTunnel engine — the core execution runtime.
///
/// In local mode, the engine runs a Source → Transform → Sink pipeline
/// in-process using a pre-parsed JSON config tree.
pub struct Engine {
    mode: ExecutionMode,
}

impl Engine {
    pub fn new(mode: ExecutionMode) -> Self {
        Engine { mode }
    }

    pub fn mode(&self) -> &ExecutionMode {
        &self.mode
    }

    /// Execute a job with an already-parsed config tree.
    pub async fn execute(&self, config: &serde_json::Value) -> anyhow::Result<()> {
        match &self.mode {
            ExecutionMode::Local => {
                tracing::info!("Executing job in LOCAL mode");
                self.execute_local(config)
            }
            ExecutionMode::Cluster { addresses } => {
                tracing::info!(
                    "Cluster mode not yet implemented, addresses: {:?}",
                    addresses
                );
                Err(anyhow::anyhow!("Cluster mode requires a running master"))
            }
        }
    }

    /// Execute locally: parse config, build pipeline, run it.
    fn execute_local(&self, config: &serde_json::Value) -> anyhow::Result<()> {
        // Determine row count from source section
        let source_rows = get_source_rows(config);

        // Build transform pipeline from config
        let transforms = build_transforms(config);

        // Determine sink type
        let sink_type = detect_sink(config);

        // Read job name
        let job_name = get_str_at(config, &["env", "job.name"])
            .unwrap_or("unnamed")
            .to_string();

        tracing::info!(
            "Job '{}' starting (rows={}, sink={})",
            job_name,
            source_rows,
            sink_type
        );

        let field_count = get_field_count(config);
        let mut total_rows = 0usize;
        for i in 0..source_rows {
            let mut row = Row::new(RowKind::Insert, field_count);
            for f in 0..field_count {
                if f == 0 {
                    row.set(f, Field::Int64(i as i64));
                } else if f == 1 {
                    row.set(f, Field::String(format!("row_{}", i)));
                } else if f == 2 {
                    row.set(f, Field::Bool(i % 2 == 0));
                } else {
                    row.set(f, Field::Null);
                }
            }

            let outputs = apply_transforms(row, &transforms);
            for out_row in outputs {
                total_rows += 1;
                if sink_type == "console" {
                    write_console(&out_row);
                }
            }
        }

        tracing::info!(
            "Job '{}' completed: {} rows processed, {} rows output",
            job_name,
            source_rows,
            total_rows
        );
        Ok(())
    }
}

/// Read the total row count from the source section of the config.
fn get_source_rows(config: &serde_json::Value) -> usize {
    if let Some(src_obj) = config.get("source") {
        let mut total = 0usize;
        if let Some(obj) = src_obj.as_object() {
            for (_, val) in obj {
                total += get_i64_at(val, &["row.num"]).unwrap_or(100) as usize;
            }
        }
        if total > 0 {
            return total;
        }
    }
    get_i64_at(config, &["env", "source.rows"]).unwrap_or(100) as usize
}

/// Read the field count from the source config's field.defs.
fn get_field_count(config: &serde_json::Value) -> usize {
    if let Some(src_obj) = config.get("source") {
        if let Some(obj) = src_obj.as_object() {
            for (_, val) in obj {
                if let Some(defs) = val.get("field.defs") {
                    if let Some(arr) = defs.as_array() {
                        return arr.len();
                    }
                }
            }
        }
    }
    // Default: read from env.source.field.count or fall back to 3
    get_i64_at(config, &["env", "source", "field", "count"]).unwrap_or(3) as usize
}

/// Build transform objects from the config's transform section.
fn build_transforms(config: &serde_json::Value) -> Vec<Box<dyn LocalTransform>> {
    let mut transforms = Vec::new();
    if let Some(transform_obj) = config.get("transform") {
        if let Some(obj) = transform_obj.as_object() {
            for (name, _cfg) in obj {
                match name.as_str() {
                    "filter" => {
                        transforms.push(Box::new(FakeFilterTransform) as Box<dyn LocalTransform>);
                    }
                    "map" => {
                        transforms.push(Box::new(FakeMapTransform) as Box<dyn LocalTransform>);
                    }
                    "rename" => {
                        transforms.push(Box::new(FakeRenameTransform) as Box<dyn LocalTransform>);
                    }
                    "select" => {
                        transforms.push(Box::new(FakeSelectTransform) as Box<dyn LocalTransform>);
                    }
                    _ => {
                        tracing::warn!("Unknown transform: {}", name);
                    }
                }
            }
        }
    }
    transforms
}

/// Detect the sink type from the config.
fn detect_sink(config: &serde_json::Value) -> &str {
    if config
        .get("sink")
        .and_then(|s| s.get("console"))
        .is_some()
    {
        "console"
    } else if config
        .get("sink")
        .and_then(|s| s.get("kafka"))
        .is_some()
    {
        "kafka"
    } else {
        "console"
    }
}

/// Apply a list of transforms to a single row.
fn apply_transforms(row: Row, transforms: &[Box<dyn LocalTransform>]) -> Vec<Row> {
    let mut current = vec![row];
    for t in transforms {
        let mut next = Vec::new();
        for r in current {
            next.extend(t.process(r));
        }
        current = next;
    }
    current
}

/// Write a row to console output.
fn write_console(row: &Row) {
    let fields: Vec<String> = row.fields.iter().map(format_field).collect();
    println!(
        "  [{}] {}",
        match row.kind {
            RowKind::Insert => "INSERT",
            RowKind::Delete => "DELETE",
            RowKind::UpdateBefore => "UPDATE_BEFORE",
            RowKind::UpdateAfter => "UPDATE_AFTER",
        },
        fields.join(", ")
    );
}

/// Format a single field value for display.
fn format_field(field: &Field) -> String {
    match field {
        Field::Null => "NULL".to_string(),
        Field::Bool(b) => b.to_string(),
        Field::Int32(i) => i.to_string(),
        Field::Int64(i) => i.to_string(),
        Field::Float32(f) => f.to_string(),
        Field::Float64(f) => f.to_string(),
        Field::String(s) => s.clone(),
        Field::Json(v) => v.to_string(),
        Field::Array(arr) => format!(
            "[{}]",
            arr.iter()
                .map(format_field)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format!("{:?}", field),
    }
}

/// Read a string value from a JSON path.
fn get_str_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for seg in path {
        current = current.get(*seg)?;
    }
    current.as_str()
}

/// Read an i64 value from a JSON path.
fn get_i64_at(value: &serde_json::Value, path: &[&str]) -> Option<i64> {
    let mut current = value;
    for seg in path {
        current = current.get(*seg)?;
    }
    current.as_i64()
}

/// Trait for local pipeline transforms.
trait LocalTransform: Send {
    fn process(&self, row: Row) -> Vec<Row>;
}

/// Filter: only keep rows where id is even.
struct FakeFilterTransform;
impl LocalTransform for FakeFilterTransform {
    fn process(&self, row: Row) -> Vec<Row> {
        if let Field::Int64(id) = row.get(0) {
            if id % 2 == 0 {
                return vec![row];
            }
        }
        vec![]
    }
}

/// Map: double the id value.
struct FakeMapTransform;
impl LocalTransform for FakeMapTransform {
    fn process(&self, mut row: Row) -> Vec<Row> {
        if let Field::Int64(id) = row.get(0) {
            row.set(0, Field::Int64(id * 2));
        }
        vec![row]
    }
}

/// Rename: swap field 0 and field 2.
struct FakeRenameTransform;
impl LocalTransform for FakeRenameTransform {
    fn process(&self, mut row: Row) -> Vec<Row> {
        if row.field_count() >= 3 {
            let f0 = row.get(0).clone();
            let f2 = row.get(2).clone();
            row.set(0, f2);
            row.set(2, f0);
        }
        vec![row]
    }
}

/// Select: keep only fields 0 and 1.
struct FakeSelectTransform;
impl LocalTransform for FakeSelectTransform {
    fn process(&self, row: Row) -> Vec<Row> {
        if row.field_count() >= 2 {
            let mut out = Row::new(row.kind, 2);
            out.set(0, row.get(0).clone());
            out.set(1, row.get(1).clone());
            vec![out]
        } else {
            vec![row]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_source_rows() {
        let config = serde_json::json!({
            "source": {
                "fake": { "row.num": 42 }
            }
        });
        assert_eq!(get_source_rows(&config), 42);

        let config2 = serde_json::json!({
            "env": { "source.rows": 99 }
        });
        assert_eq!(get_source_rows(&config2), 99);
    }

    #[test]
    fn test_filter_transform() {
        let t = FakeFilterTransform;
        let mut row_even = Row::new(RowKind::Insert, 1);
        row_even.set(0, Field::Int64(4));
        let mut row_odd = Row::new(RowKind::Insert, 1);
        row_odd.set(0, Field::Int64(3));
        assert_eq!(t.process(row_even).len(), 1);
        assert_eq!(t.process(row_odd).len(), 0);
    }

    #[test]
    fn test_map_transform() {
        let t = FakeMapTransform;
        let mut row = Row::new(RowKind::Insert, 1);
        row.set(0, Field::Int64(7));
        let out = t.process(row);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get(0), &Field::Int64(14));
    }

    #[test]
    fn test_rename_transform() {
        let t = FakeRenameTransform;
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, Field::Int64(1));
        row.set(1, Field::String("mid".to_string()));
        row.set(2, Field::Bool(true));
        let out = t.process(row);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get(0), &Field::Bool(true));
        assert_eq!(out[0].get(2), &Field::Int64(1));
    }

    #[test]
    fn test_select_transform() {
        let t = FakeSelectTransform;
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, Field::Int64(1));
        row.set(1, Field::String("hello".to_string()));
        row.set(2, Field::Bool(true));
        let out = t.process(row);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].field_count(), 2);
        assert_eq!(out[0].get(0), &Field::Int64(1));
        assert_eq!(out[0].get(1), &Field::String("hello".to_string()));
    }

    #[tokio::test]
    async fn test_engine_execute_local_minimal() {
        let engine = Engine::new(ExecutionMode::Local);
        let config = serde_json::json!({
            "env": { "job.name": "test-engine" },
            "source": { "fake": { "row.num": 3 } },
            "sink": { "console": { "format": "json" } }
        });
        assert!(engine.execute(&config).await.is_ok());
    }

    #[tokio::test]
    async fn test_engine_execute_with_transform() {
        let engine = Engine::new(ExecutionMode::Local);
        let config = serde_json::json!({
            "env": { "job.name": "filter-test" },
            "source": { "fake": { "row.num": 4 } },
            "transform": { "filter": { "rule": "active" } },
            "sink": { "console": { "format": "json" } }
        });
        assert!(engine.execute(&config).await.is_ok());
    }
}
