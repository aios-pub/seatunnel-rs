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

//! Canal-client style JSON format.
//!
//! Reproduces the message shape of the Java `AbstractCanalClient`
//! pipeline (canal binlog → Kafka):
//!
//! ```json
//! {
//!   "requestId": "32-hex UUID without dashes",
//!   "dbName": "lowercase database name",
//!   "tableName": "lowercase table name",
//!   "eventType": "insert | update | delete",
//!   "data":    { "<target field>": <converted value>, ... },
//!   "oldData": { ... update events only ... }
//! }
//! ```
//!
//! Transformation rules (faithful to the Java implementation):
//! - table names are looked up in the per-table field config by their
//!   camelCase form (`l_class_student` → `lClassStudent`, first letter
//!   NOT capitalized);
//! - `must` fields are always mapped, `update` fields only when the
//!   column actually changed;
//! - update events where no configured column changed are DROPPED
//!   (the Java `pushJson.clear()` filter); deletes are never filtered;
//! - value conversion: strict `yyyy-MM-dd HH:mm:ss` dates → epoch
//!   millis number; `"0"` or zero-free-leading digit strings → long;
//!   everything else stays a string;
//! - `oldData` (update only) carries every configured column from the
//!   before-image regardless of the changed flag;
//! - the Kafka partition key is the configured primary-key value.
//!
//! The CDC sources emit UPDATE as a delete(before) + insert(after) row
//! pair; [`CanalClientEncoder`] is a STATEFUL encoder that pairs
//! adjacent rows with the same key back into a single `update` message.

use chrono::NaiveDateTime;
use seatunnel_api::row::{Field, Row, RowKind};
use seatunnel_api::schema::TableSchema;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::error::Error;

/// Per-table field configuration (Java `subTableFieldsJson` entry).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TableFields {
    /// Primary-key column name (drives the Kafka partition key).
    #[serde(default)]
    pub key: String,
    /// db column → target field; always mapped.
    #[serde(default)]
    pub must: HashMap<String, String>,
    /// db column → target field; mapped only when the column changed.
    #[serde(default)]
    pub update: HashMap<String, String>,
}

/// Full canal-client format configuration.
#[derive(Debug, Clone)]
pub struct CanalClientConfig {
    /// Database name (message `dbName`, lowercased).
    pub database_name: String,
    /// Table name (message `tableName`, lowercased; config lookup by its
    /// camelCase form).
    pub table_name: String,
    /// Positional → db column names (CDC rows arrive positionally).
    pub columns: Vec<String>,
    /// camelCase(table) → field config.
    pub tables: HashMap<String, TableFields>,
}

/// snake_case → camelCase with the FIRST letter left untouched
/// (Java `CommonUtils.camelCaseName`): `l_class_student` → `lClassStudent`.
pub fn camel_case_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper_next = false;
    for ch in name.chars() {
        if ch == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(ch.to_uppercase());
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

/// Java `isNumber`: exactly `"0"` or a non-empty digit string that does
/// not start with '0' (preserves leading zeros as strings).
pub fn is_java_number(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    !value.is_empty()
        && !value.starts_with('0')
        && value.bytes().all(|b| b.is_ascii_digit())
}

/// Java `isValidDate` + conversion: strict `yyyy-MM-dd HH:mm:ss` parse
/// (lenient=false) → epoch millis string; anything else → unchanged.
fn valid_date_to_millis(value: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().timestamp_millis().checked_add(0))
}

/// Convert one field through the Java value rules.
pub fn convert_field(field: &Field) -> Value {
    match field {
        Field::Null => Value::Null,
        Field::Bool(b) => Value::Bool(*b),
        Field::Int8(v) => (*v as i64).into(),
        Field::Int16(v) => (*v as i64).into(),
        Field::Int32(v) => (*v).into(),
        Field::Int64(v) => (*v).into(),
        Field::UInt8(v) => (*v as u64).into(),
        Field::UInt16(v) => (*v as u64).into(),
        Field::UInt32(v) => (*v as u64).into(),
        Field::UInt64(v) => (*v).into(),
        Field::Float32(v) => Number::from_f64(*v as f64).map(Value::Number).unwrap_or(Value::Null),
        Field::Float64(v) => Number::from_f64(*v).map(Value::Number).unwrap_or(Value::Null),
        Field::Decimal(d) => Value::String(d.to_string()),
        Field::String(s) => convert_string(s),
        Field::Date(d) => date_to_millis(
            NaiveDateTime::parse_from_str(&format!("{} 00:00:00", d.format("%Y-%m-%d")), "%Y-%m-%d %H:%M:%S")
                .unwrap_or_default(),
        ),
        Field::DateTime(dt) => date_to_millis(*dt),
        Field::TimestampTz(ts) => Value::Number(Number::from(ts.timestamp_millis())),
        Field::Time(t) => Value::String(t.format("%H:%M:%S").to_string()),
        Field::Duration(ns) => (*ns).into(),
        Field::Bytes(b) => Value::String(hex::encode(b)),
        Field::Json(j) => j.clone(),
        Field::Array(items) => Value::Array(items.iter().map(convert_field).collect()),
        Field::Row(fields) => Value::Array(fields.iter().map(convert_field).collect()),
    }
}

use serde_json::Number;

fn date_to_millis(dt: NaiveDateTime) -> Value {
    match dt.and_utc().timestamp_millis().checked_add(0) {
        Some(millis) => Value::Number(Number::from(millis)),
        None => Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
    }
}

fn convert_string(s: &str) -> Value {
    if let Some(millis) = valid_date_to_millis(s) {
        return Value::Number(Number::from(millis));
    }
    if is_java_number(s) {
        if let Ok(long) = s.parse::<i64>() {
            return Value::Number(Number::from(long));
        }
    }
    Value::String(s.to_string())
}

/// Before-image held until its after-image arrives. Held images older
/// than [`PAIRING_WINDOW`] are emitted as real deletes.
struct PendingDelete {
    key: String,
    source_row: Row,
    data: Map<String, Value>,
    since: std::time::Instant,
}

/// How long a before-image may wait for its after-image before being
/// emitted as a standalone delete (the CDC sources emit the pair
/// back-to-back, well inside this window).
pub const PAIRING_WINDOW: std::time::Duration = std::time::Duration::from_secs(2);

/// Stateful encoder: maps rows into canal-client messages, pairing the
/// CDC delete(before)+insert(after) row pairs of one UPDATE into a single
/// `update` message with `oldData`.
pub struct CanalClientEncoder {
    config: CanalClientConfig,
    fields: TableFields,
    column_positions: HashMap<String, usize>,
    /// Adjacent before-image awaiting its after-image (or flush).
    pending: Option<PendingDelete>,
}

impl CanalClientEncoder {
    pub fn new(config: CanalClientConfig) -> anyhow::Result<Self> {
        let camel = camel_case_name(&config.table_name);
        let fields = config
            .tables
            .get(&camel)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "canal-client config has no field mapping for table '{}'",
                    camel
                )
            })?;
        let column_positions: HashMap<String, usize> = config
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.clone(), i))
            .collect();
        if fields.must.is_empty() && fields.update.is_empty() {
            anyhow::bail!(
                "canal-client table '{}' has empty must/update mappings",
                camel
            );
        }
        Ok(CanalClientEncoder {
            config,
            fields,
            column_positions,
            pending: None,
        })
    }

    fn get<'r>(&self, row: &'r Row, column: &str) -> Option<&'r Field> {
        self.column_positions
            .get(column)
            .and_then(|i| row.fields.get(*i))
    }

    /// Kafka partition key: the configured primary-key value.
    fn row_key(&self, row: &Row) -> String {
        self.get(row, &self.fields.key)
            .map(field_to_key)
            .unwrap_or_default()
    }

    /// Map a row image into `{target: value}`.
    /// `old` (update pairing) drives the changed checks; without it
    /// (insert / single-image update) every mapped field is included —
    /// canal marks inserted columns updated=true.
    fn map_image(&self, row: &Row, old: Option<&Row>) -> (Map<String, Value>, bool) {
        let mut out = Map::new();
        let mut is_update = false;
        for (col, target) in &self.fields.must {
            if let Some(field) = self.get(row, col) {
                let value = convert_field(field);
                if old.is_some_and(|o| {
                    self.get(o, col).map(convert_field) != Some(value.clone())
                }) {
                    is_update = true;
                }
                out.insert(target.clone(), value);
            }
        }
        for (col, target) in &self.fields.update {
            let changed = old.is_some_and(|o| {
                self.get(o, col).map(convert_field) != self.get(row, col).map(convert_field)
            });
            if old.is_none() || changed {
                if let Some(field) = self.get(row, col) {
                    out.insert(target.clone(), convert_field(field));
                }
            }
            if changed {
                is_update = true;
            }
        }
        (out, is_update)
    }

    /// `oldData`: every configured column from the before-image, no
    /// changed-flag check (Java behavior).
    fn map_old(&self, old: &Row) -> Map<String, Value> {
        let mut out = Map::new();
        for (col, target) in self.fields.must.iter().chain(self.fields.update.iter()) {
            if let Some(field) = self.get(old, col) {
                out.insert(target.clone(), convert_field(field));
            }
        }
        out
    }

    fn build_message(
        &self,
        event_type: &str,
        key: &str,
        data: Map<String, Value>,
        old_data: Option<Map<String, Value>>,
    ) -> (String, String) {
        let mut message = Map::new();
        message.insert(
            "requestId".into(),
            Value::String(uuid::Uuid::new_v4().simple().to_string()),
        );
        message.insert(
            "dbName".into(),
            Value::String(self.config.database_name.to_lowercase()),
        );
        message.insert(
            "tableName".into(),
            Value::String(self.config.table_name.to_lowercase()),
        );
        message.insert("eventType".into(), Value::String(event_type.to_string()));
        message.insert("data".into(), Value::Object(data));
        if let Some(old) = old_data {
            message.insert("oldData".into(), Value::Object(old));
        }
        (key.to_string(), Value::Object(message).to_string())
    }

    fn stash_delete(&mut self, row: &Row) -> (String, String) {
        // Delete data carries the must fields only (canal before-images
        // carry no changed flags, so update fields stay out).
        let (data, _) = {
            let mut out = Map::new();
            for (col, target) in &self.fields.must {
                if let Some(field) = self.get(row, col) {
                    out.insert(target.clone(), convert_field(field));
                }
            }
            (out, ())
        };
        let pending = PendingDelete {
            key: self.row_key(row),
            source_row: row.clone(),
            data,
            since: std::time::Instant::now(),
        };
        let message = self.build_message("delete", &pending.key, pending.data.clone(), None);
        self.pending = Some(pending);
        message
    }

    /// Encode one row; returns 0..2 messages `(kafka_key, payload)`.
    /// Zero = filtered (update without any configured column change, or
    /// a before-image held for pairing).
    pub fn encode(&mut self, row: &Row) -> anyhow::Result<Vec<(String, String)>> {
        let key = self.row_key(row);
        match row.kind {
            RowKind::Delete | RowKind::UpdateBefore => {
                // A previous before-image that never paired was a real
                // delete — emit it, then hold the current one.
                let mut messages = Vec::new();
                if let Some(pending) = self.pending.take() {
                    messages.push(self.build_message("delete", &pending.key, pending.data, None));
                }
                self.stash_delete(row);
                Ok(messages)
            }
            RowKind::Insert | RowKind::UpdateAfter => {
                let mut messages = Vec::new();
                let paired = match self.pending.take() {
                    Some(pending) if pending.key == key => Some(pending),
                    Some(pending) => {
                        // Different key: the stashed row was a real delete.
                        messages.push(
                            self.build_message("delete", &pending.key, pending.data, None),
                        );
                        None
                    }
                    None => None,
                };
                let current = match paired {
                    Some(pending) => {
                        // One UPDATE message: data = after image (changed
                        // checks against the before image), oldData = all
                        // configured columns of the before image.
                        let (data, is_update) = self.map_image(row, Some(&pending.source_row));
                        if !self.fields.update.is_empty() && !is_update {
                            None // Java filter: update without configured changes
                        } else {
                            let old_data = self.map_old(&pending.source_row);
                            Some(self.build_message("update", &key, data, Some(old_data)))
                        }
                    }
                    None => {
                        // INSERT; a lone UpdateAfter (no before-image
                        // available) serializes as update without oldData.
                        let event = if row.kind == RowKind::UpdateAfter {
                            "update"
                        } else {
                            "insert"
                        };
                        let (data, _) = self.map_image(row, None);
                        Some(self.build_message(event, &key, data, None))
                    }
                };
                messages.extend(current);
                Ok(messages)
            }
        }
    }

    /// Flush any held before-image (job close / bounded end).
    pub fn flush(&mut self) -> Vec<(String, String)> {
        match self.pending.take() {
            Some(pending) => vec![
                self.build_message("delete", &pending.key, pending.data, None),
            ],
            None => Vec::new(),
        }
    }

    /// Emit a held before-image as a real delete once the pairing window
    /// expired (called from each sink flush cycle).
    pub fn expire_pending(&mut self) -> Vec<(String, String)> {
        match &self.pending {
            Some(pending) if pending.since.elapsed() >= PAIRING_WINDOW => {}
            _ => return Vec::new(),
        }
        self.flush()
    }
}

/// Deserialize a canal-client message into rows (source side): fields are
/// pulled from `data` by schema column name; delete → RowKind::Delete.
pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let event_type = value.get("eventType").and_then(|v| v.as_str()).unwrap_or("insert");
    let kind = match event_type {
        "update" => RowKind::UpdateAfter,
        "delete" => RowKind::Delete,
        _ => RowKind::Insert,
    };
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let mut row = Row::new(kind, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        let field = data.get(&col.name).map(json_value_to_field).unwrap_or(Field::Null);
        row.set(i, field);
    }
    Ok(vec![row])
}

fn json_value_to_field(v: &Value) -> Field {
    match v {
        Value::Null => Field::Null,
        Value::Bool(b) => Field::Bool(*b),
        Value::Number(n) => n
            .as_i64()
            .map(Field::Int64)
            .or_else(|| n.as_f64().map(Field::Float64))
            .unwrap_or(Field::Null),
        Value::String(s) => Field::String(s.clone()),
        other => Field::String(other.to_string()),
    }
}

fn field_to_key(field: &Field) -> String {
    match field {
        Field::String(s) => s.clone(),
        other => match other {
            Field::Bool(b) => Value::Bool(*b).to_string(),
            Field::Int8(v) => v.to_string(),
            Field::Int16(v) => v.to_string(),
            Field::Int32(v) => v.to_string(),
            Field::Int64(v) => v.to_string(),
            Field::UInt8(v) => v.to_string(),
            Field::UInt16(v) => v.to_string(),
            Field::UInt32(v) => v.to_string(),
            Field::UInt64(v) => v.to_string(),
            Field::Float32(v) => v.to_string(),
            Field::Float64(v) => v.to_string(),
            _ => String::new(),
        },
    }
}

/// Single-row stateless serialization (formats::serialize fallback):
/// update/delete rows serialize their image; no pairing or filtering.
pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut message = Map::new();
    message.insert(
        "requestId".to_string(),
        Value::String(uuid::Uuid::new_v4().simple().to_string()),
    );
    message.insert("dbName".to_string(), Value::String(String::new()));
    message.insert("tableName".to_string(), Value::String(String::new()));
    let (event_type, _) = match row.kind {
        RowKind::Insert => ("insert", ()),
        RowKind::UpdateAfter => ("update", ()),
        RowKind::UpdateBefore => ("update", ()),
        RowKind::Delete => ("delete", ()),
    };
    message.insert("eventType".to_string(), Value::String(event_type.to_string()));
    let mut data = Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        data.insert(col.name.clone(), convert_field(row.fields.get(i).unwrap_or(&Field::Null)));
    }
    message.insert("data".to_string(), Value::Object(data));
    Ok(serde_json::to_vec(&Value::Object(message))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_case() {
        assert_eq!(camel_case_name("l_class_student"), "lClassStudent");
        assert_eq!(camel_case_name("users"), "users");
        assert_eq!(camel_case_name("a_b_c"), "aBC");
    }

    #[test]
    fn test_java_number_rules() {
        assert!(is_java_number("0"));
        assert!(is_java_number("1001"));
        assert!(!is_java_number("0123")); // leading zero stays a string
        assert!(!is_java_number("-5"));
        assert!(!is_java_number("1.5"));
        assert!(!is_java_number(""));
        assert!(!is_java_number("12a"));
    }

    #[test]
    fn test_string_conversion() {
        // Strict date → epoch millis (2024-05-06 07:08:09 UTC).
        let v = convert_string("2024-05-06 07:08:09");
        assert_eq!(v, Value::Number(Number::from(1714979289000i64)));
        // Non-strict date stays a string.
        assert_eq!(convert_string("2024-05-06"), Value::String("2024-05-06".into()));
        assert_eq!(convert_string("2024-13-40 00:00:00"), Value::String("2024-13-40 00:00:00".into()));
        // Numbers.
        assert_eq!(convert_string("1001"), Value::Number(Number::from(1001i64)));
        assert_eq!(convert_string("0123"), Value::String("0123".into()));
    }

    fn fields_config() -> CanalClientConfig {
        let tables: HashMap<String, TableFields> =
            serde_json::from_str(
                r#"{"lClassStudent": {"key": "id",
                     "must": {"id": "id", "name": "name"},
                     "update": {"status": "status"}}}"#,
            )
            .unwrap();
        CanalClientConfig {
            database_name: "NewOriental_Data_Recommand".into(),
            table_name: "l_class_student".into(),
            columns: vec!["id".into(), "name".into(), "status".into(), "other".into()],
            tables,
        }
    }

    fn row_of(kind: RowKind, id: i64, name: &str, status: i64) -> Row {
        let mut row = Row::new(kind, 4);
        row.set(0, Field::Int64(id));
        row.set(1, Field::String(name.into()));
        row.set(2, Field::Int64(status));
        row.set(3, Field::String("ignored".into()));
        row
    }

    #[test]
    fn test_encoder_insert_maps_must_and_update_fields() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        let messages = encoder.encode(&row_of(RowKind::Insert, 1001, "张三", 1)).unwrap();
        assert_eq!(messages.len(), 1);
        let (key, payload) = &messages[0];
        assert_eq!(key, "1001");
        let json: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(json["dbName"], "neworiental_data_recommand");
        assert_eq!(json["tableName"], "l_class_student");
        assert_eq!(json["eventType"], "insert");
        assert_eq!(json["data"]["id"], 1001);
        assert_eq!(json["data"]["name"], "张三");
        assert_eq!(json["data"]["status"], 1);
        assert!(json["data"].get("other").is_none());
        assert!(json.get("oldData").is_none());
        assert_eq!(json["requestId"].as_str().unwrap().len(), 32);
    }

    #[test]
    fn test_encoder_pairs_delete_insert_into_update() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        // mysql-cdc emits UPDATE as Delete(before) + Insert(after).
        let held = encoder
            .encode(&row_of(RowKind::Delete, 1001, "李四", 0))
            .unwrap();
        assert!(held.is_empty(), "before-image is held for pairing");
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 1001, "张三", 0))
            .unwrap();
        assert_eq!(messages.len(), 1);
        let json: Value = serde_json::from_str(&messages[0].1).unwrap();
        assert_eq!(json["eventType"], "update");
        assert_eq!(json["data"]["name"], "张三");
        assert_eq!(json["oldData"]["name"], "李四");
        // status unchanged (0→0) → not mapped into data, but IS in oldData.
        assert!(json["data"].get("status").is_none());
        assert_eq!(json["oldData"]["status"], 0);
    }

    #[test]
    fn test_encoder_filters_update_without_configured_changes() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        // Only 'other' (not configured) changes between the images.
        encoder.encode(&row_of(RowKind::Delete, 1001, "same", 0)).unwrap();
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 1001, "same", 0))
            .unwrap();
        assert!(messages.is_empty(), "update without configured changes must be filtered");
    }

    #[test]
    fn test_encoder_real_delete_flushes_before_unrelated_insert() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        encoder.encode(&row_of(RowKind::Delete, 7, "gone", 0)).unwrap();
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 1001, "new", 1))
            .unwrap();
        assert_eq!(messages.len(), 2);
        let delete: Value = serde_json::from_str(&messages[0].1).unwrap();
        assert_eq!(delete["eventType"], "delete");
        assert_eq!(delete["data"]["id"], 7);
        // Delete data carries must fields only (no status).
        assert!(delete["data"].get("status").is_none());
        let insert: Value = serde_json::from_str(&messages[1].1).unwrap();
        assert_eq!(insert["eventType"], "insert");
    }

    #[test]
    fn test_encoder_flush_emits_trailing_delete() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        encoder.encode(&row_of(RowKind::Delete, 9, "tail", 0)).unwrap();
        let flushed = encoder.flush();
        assert_eq!(flushed.len(), 1);
        let json: Value = serde_json::from_str(&flushed[0].1).unwrap();
        assert_eq!(json["eventType"], "delete");
        assert_eq!(json["data"]["id"], 9);
    }
}
