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
//! Schema-driven mode ([`CanalClientEncoder::from_schema`], no explicit
//! `canal-client.columns`): the effective mapping starts from the
//! identity baseline (every schema column identity-mapped, no update
//! filter, schema primary key as partition key) and the table's
//! `sub-table-fields` entry applies DIFFERENCES ONLY — `must` entries
//! rename their column's target, `update` entries move their column to
//! changed-only, `key` overrides the partition key. Full Java
//! `subTableFields` parity (authoritative entry + change filtering)
//! requires the explicit positional `canal-client.columns` config.
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

/// Timezone used to interpret naive datetime strings (`yyyy-MM-dd
/// HH:mm:ss`), mirroring Java `SimpleDateFormat` with the JVM default
/// timezone: the default is the SERVER's local timezone.
#[derive(Debug, Clone, Copy, Default)]
pub enum ServerTz {
    /// System local timezone (default, matches Java behavior).
    #[default]
    Local,
    Utc,
    Fixed(chrono::FixedOffset),
    Named(chrono_tz::Tz),
}

impl ServerTz {
    /// Accepts `local`/empty (default), `UTC`/`GMT`/`Z`, fixed offsets
    /// (`+08:00`, `-05:30`) and IANA names (`Asia/Shanghai`).
    pub fn parse(value: &str) -> ServerTz {
        let t = value.trim();
        if t.is_empty() || t.eq_ignore_ascii_case("local") || t.eq_ignore_ascii_case("system") {
            return ServerTz::Local;
        }
        if t.eq_ignore_ascii_case("utc") || t.eq_ignore_ascii_case("gmt") || t == "Z" {
            return ServerTz::Utc;
        }
        if let Some(offset) = parse_fixed_offset(t) {
            return ServerTz::Fixed(offset);
        }
        if let Ok(tz) = t.parse::<chrono_tz::Tz>() {
            return ServerTz::Named(tz);
        }
        tracing::warn!(
            "canal-client: unrecognized server time zone '{}', using the system local zone",
            t
        );
        ServerTz::Local
    }

    /// Epoch millis of a naive datetime interpreted in this zone. DST
    /// gaps fall back to the earliest instant (Java lenient-adjacent).
    pub fn naive_to_millis(&self, dt: NaiveDateTime) -> Option<i64> {
        match self {
            ServerTz::Utc => Some(dt.and_utc().timestamp_millis()),
            // DST gaps fall back to the earliest instant.
            ServerTz::Local => dt
                .and_local_timezone(chrono::Local)
                .earliest()
                .map(|aware| aware.timestamp_millis()),
            ServerTz::Fixed(offset) => dt
                .and_local_timezone(*offset)
                .earliest()
                .map(|aware| aware.timestamp_millis()),
            ServerTz::Named(tz) => dt
                .and_local_timezone(*tz)
                .earliest()
                .map(|aware| aware.timestamp_millis()),
        }
    }
}

/// Parse `+HH:MM` / `-HH:MM` (also `+HHMM` / `+HH`).
fn parse_fixed_offset(value: &str) -> Option<chrono::FixedOffset> {
    let bytes = value.as_bytes();
    if bytes.len() < 3 || (bytes[0] != b'+' && bytes[0] != b'-') {
        return None;
    }
    let (h, m) = match bytes.len() {
        // +HH
        3 => (value[1..3].parse::<i32>().ok()?, 0),
        // +HHMM
        5 => (
            value[1..3].parse::<i32>().ok()?,
            value[3..5].parse::<i32>().ok()?,
        ),
        // +HH:MM
        6 => (
            value[1..3].parse::<i32>().ok()?,
            value[4..6].parse::<i32>().ok()?,
        ),
        _ => return None,
    };
    let seconds = h * 3600 + m * 60;
    if bytes[0] == b'-' {
        chrono::FixedOffset::east_opt(-seconds)
    } else {
        chrono::FixedOffset::east_opt(seconds)
    }
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
    /// Timezone for naive datetime interpretation (default: the server's
    /// local zone, mirroring Java's SimpleDateFormat default).
    pub server_time_zone: String,
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
    !value.is_empty() && !value.starts_with('0') && value.bytes().all(|b| b.is_ascii_digit())
}

/// Java `isValidDate` + conversion: strict `yyyy-MM-dd HH:mm:ss` parse
/// (lenient=false) → epoch millis interpreted in `tz`; anything else →
/// unchanged.
fn valid_date_to_millis(tz: &ServerTz, value: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| tz.naive_to_millis(dt))
}

/// Convert one field through the Java value rules with the server
/// timezone applied to naive datetime strings.
pub fn convert_field(tz: &ServerTz, field: &Field) -> Value {
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
        Field::Float32(v) => Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Float64(v) => Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Decimal(d) => Value::String(d.to_string()),
        Field::String(s) => convert_string(tz, s),
        Field::Date(d) => NaiveDateTime::parse_from_str(
            &format!("{} 00:00:00", d.format("%Y-%m-%d")),
            "%Y-%m-%d %H:%M:%S",
        )
        .ok()
        .and_then(|dt| tz.naive_to_millis(dt))
        .map(|millis| Value::Number(Number::from(millis)))
        .unwrap_or(Value::String(d.to_string())),
        Field::DateTime(dt) => tz
            .naive_to_millis(*dt)
            .map(|millis| Value::Number(Number::from(millis)))
            .unwrap_or(Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())),
        Field::TimestampTz(ts) => Value::Number(Number::from(ts.timestamp_millis())),
        Field::Time(t) => Value::String(t.format("%H:%M:%S").to_string()),
        Field::Duration(ns) => (*ns).into(),
        Field::Bytes(b) => Value::String(hex::encode(b)),
        Field::Json(j) => j.clone(),
        Field::Array(items) => Value::Array(items.iter().map(|f| convert_field(tz, f)).collect()),
        Field::Row(fields) => Value::Array(fields.iter().map(|f| convert_field(tz, f)).collect()),
    }
}

use serde_json::Number;

fn convert_string(tz: &ServerTz, s: &str) -> Value {
    if let Some(millis) = valid_date_to_millis(tz, s) {
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
#[derive(Debug)]
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

/// One encoded canal-client message, bound to its origin table identity
/// (`database.table`) so routing sinks can pick a topic per table.
#[derive(Debug, Clone, PartialEq)]
pub struct CanalMessage {
    /// Origin `database.table` of the row that produced this message.
    pub table: String,
    /// Kafka partition key.
    pub key: String,
    /// JSON payload.
    pub payload: String,
}

/// Per-table encoding state: the identity stamped into messages, the
/// resolved field mapping, positional columns and the update-pairing
/// pending slot (isolated per table so same-key rows of different
/// tables never mis-pair).
#[derive(Debug)]
struct TableState {
    db_name: String,
    table_name: String,
    fields: TableFields,
    tz: ServerTz,
    column_positions: HashMap<String, usize>,
    /// Adjacent before-image awaiting its after-image (or flush).
    pending: Option<PendingDelete>,
}

impl TableState {
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
                let value = convert_field(&self.tz, field);
                if old.is_some_and(|o| {
                    self.get(o, col).map(|f| convert_field(&self.tz, f)) != Some(value.clone())
                }) {
                    is_update = true;
                }
                out.insert(target.clone(), value);
            }
        }
        for (col, target) in &self.fields.update {
            let changed = old.is_some_and(|o| {
                self.get(o, col).map(|f| convert_field(&self.tz, f))
                    != self.get(row, col).map(|f| convert_field(&self.tz, f))
            });
            if old.is_none() || changed {
                if let Some(field) = self.get(row, col) {
                    out.insert(target.clone(), convert_field(&self.tz, field));
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
                out.insert(target.clone(), convert_field(&self.tz, field));
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
    ) -> CanalMessage {
        let mut message = Map::new();
        message.insert(
            "requestId".into(),
            Value::String(uuid::Uuid::new_v4().simple().to_string()),
        );
        message.insert("dbName".into(), Value::String(self.db_name.to_lowercase()));
        message.insert(
            "tableName".into(),
            Value::String(self.table_name.to_lowercase()),
        );
        message.insert("eventType".into(), Value::String(event_type.to_string()));
        message.insert("data".into(), Value::Object(data));
        if let Some(old) = old_data {
            message.insert("oldData".into(), Value::Object(old));
        }
        CanalMessage {
            table: format!("{}.{}", self.db_name, self.table_name),
            key: key.to_string(),
            payload: Value::Object(message).to_string(),
        }
    }

    fn stash_delete(&mut self, row: &Row) -> CanalMessage {
        // Delete data carries the must fields only (canal before-images
        // carry no changed flags, so update fields stay out).
        let mut data = Map::new();
        for (col, target) in &self.fields.must {
            if let Some(field) = self.get(row, col) {
                data.insert(target.clone(), convert_field(&self.tz, field));
            }
        }
        let key = self.row_key(row);
        let message = self.build_message("delete", &key, data.clone(), None);
        self.pending = Some(PendingDelete {
            key,
            source_row: row.clone(),
            data,
            since: std::time::Instant::now(),
        });
        message
    }

    /// Encode one row; returns 0..2 messages.
    /// Zero = filtered (update without any configured column change, or
    /// a before-image held for pairing).
    fn encode(&mut self, row: &Row) -> Vec<CanalMessage> {
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
                messages
            }
            RowKind::Insert | RowKind::UpdateAfter => {
                let mut messages = Vec::new();
                let paired = match self.pending.take() {
                    Some(pending) if pending.key == key => Some(pending),
                    Some(pending) => {
                        // Different key: the stashed row was a real delete.
                        messages.push(self.build_message(
                            "delete",
                            &pending.key,
                            pending.data,
                            None,
                        ));
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
                messages
            }
        }
    }

    /// Flush any held before-image (job close / bounded end).
    fn flush(&mut self) -> Vec<CanalMessage> {
        match self.pending.take() {
            Some(pending) => vec![self.build_message("delete", &pending.key, pending.data, None)],
            None => Vec::new(),
        }
    }

    /// Emit a held before-image as a real delete once the pairing window
    /// expired (called from each sink flush cycle).
    fn expire_pending(&mut self) -> Vec<CanalMessage> {
        match &self.pending {
            Some(pending) if pending.since.elapsed() >= PAIRING_WINDOW => {}
            _ => return Vec::new(),
        }
        self.flush()
    }
}

/// Stateful multi-table encoder: maps rows into canal-client messages,
/// pairing the CDC delete(before)+insert(after) row pairs of one UPDATE
/// into a single `update` message with `oldData`.
///
/// Rows carrying [`Row::origin_table`] are encoded against their own
/// table's state (identity, columns, pairing slot); rows without origin
/// fall back to the static default table for single-table backward
/// compatibility.
#[derive(Debug)]
pub struct CanalClientEncoder {
    config: CanalClientConfig,
    tz: ServerTz,
    /// `database.table` → per-table state.
    tables: HashMap<String, TableState>,
    /// State serving rows without origin identity: the explicit config
    /// table, or (schema-driven mode) the FIRST registered schema.
    default_table: String,
    /// Built from explicit `canal-client.columns`: single-table
    /// authoritative — rows from other tables are rejected instead of
    /// being silently stamped with the wrong identity.
    explicit: bool,
}

impl CanalClientEncoder {
    pub fn new(config: CanalClientConfig) -> anyhow::Result<Self> {
        let tz = ServerTz::parse(&config.server_time_zone);
        let camel = camel_case_name(&config.table_name);
        let fields = config.tables.get(&camel).cloned().ok_or_else(|| {
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
        let default_table = format!("{}.{}", config.database_name, config.table_name);
        let mut tables = HashMap::new();
        tables.insert(
            default_table.clone(),
            TableState {
                db_name: config.database_name.clone(),
                table_name: config.table_name.clone(),
                fields,
                tz,
                column_positions,
                pending: None,
            },
        );
        Ok(CanalClientEncoder {
            config,
            tz,
            tables,
            default_table,
            explicit: true,
        })
    }

    /// Empty schema-driven encoder: tables register themselves as their
    /// initial-schema events arrive (see [`Self::register_schema`]).
    pub fn new_auto(config: CanalClientConfig) -> Self {
        CanalClientEncoder {
            tz: ServerTz::parse(&config.server_time_zone),
            config,
            tables: HashMap::new(),
            default_table: String::new(),
            explicit: false,
        }
    }

    /// Schema-driven constructor (automatic column mapping + delta
    /// overrides) for a SINGLE table; see [`Self::register_schema`] for
    /// what the mapping resolves to.
    pub fn from_schema(config: CanalClientConfig, schema: &TableSchema) -> anyhow::Result<Self> {
        let mut encoder = Self::new_auto(config);
        encoder.register_schema(schema)?;
        Ok(encoder)
    }

    /// Register (or idempotently re-register) one table's schema.
    ///
    /// `canal-client.columns` absent → the positional column list is taken
    /// from `schema` (information_schema ordinal order — the order CDC
    /// rows arrive in) and the effective field mapping is built from the
    /// identity baseline with the table's `sub-table-fields` entry (when
    /// present) applied as DIFFERENCES ONLY:
    ///
    /// - baseline: EVERY schema column identity-mapped into `must`
    ///   (always present in `data`), empty `update` (which disables the
    ///   no-configured-change update filter — full pass-through, like
    ///   the Java `LessonCanalClient`), partition key = the schema
    ///   primary key;
    /// - `must` entries override the target of their column (rename),
    ///   the column stays always-present;
    /// - `update` entries MOVE their column from the always-present set
    ///   to the changed-only set (target rename allowed) and enable the
    ///   Java update filter (an update changing no configured column is
    ///   dropped);
    /// - `key` overrides the partition key column.
    ///
    /// Columns or keys named by the entry but absent from the schema are
    /// configuration mistakes and fail loudly. The FIRST registered
    /// schema also serves rows without origin identity (backward
    /// compatibility with single-table pipelines).
    pub fn register_schema(&mut self, schema: &TableSchema) -> anyhow::Result<()> {
        let identifier = schema.table_identifier.clone();
        if self.tables.contains_key(&identifier) {
            return Ok(()); // replayed initial-schema event
        }
        let (db_name, table_name) = split_table_identifier(&identifier);
        let db_name = db_name.to_string();
        let table_name = table_name.to_string();
        let camel = camel_case_name(&table_name);
        let schema_key = || {
            schema
                .primary_key_columns()
                .first()
                .cloned()
                .or_else(|| schema.columns.first().map(|c| c.name.clone()))
                .unwrap_or_default()
        };
        let fields = match self.config.tables.get(&camel).cloned() {
            None => TableFields {
                key: schema_key(),
                must: schema
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.name.clone()))
                    .collect(),
                update: HashMap::new(),
            },
            Some(fields) => {
                let known = |name: &str| schema.columns.iter().any(|c| c.name == name);
                for name in fields.must.keys().chain(fields.update.keys()) {
                    if !known(name) {
                        anyhow::bail!(
                            "canal-client table '{}': mapped column '{}' is not in the source schema",
                            camel,
                            name
                        );
                    }
                }
                if !fields.key.is_empty() && !known(&fields.key) {
                    anyhow::bail!(
                        "canal-client table '{}': key column '{}' is not in the source schema",
                        camel,
                        fields.key
                    );
                }
                // Identity baseline with the explicit deltas applied:
                // `update` columns become changed-only, `must` renames,
                // everything else stays identity always-present.
                let mut must = HashMap::new();
                let mut update = HashMap::new();
                for col in &schema.columns {
                    if let Some(target) = fields.update.get(&col.name) {
                        update.insert(col.name.clone(), target.clone());
                    } else {
                        let target = fields
                            .must
                            .get(&col.name)
                            .cloned()
                            .unwrap_or_else(|| col.name.clone());
                        must.insert(col.name.clone(), target);
                    }
                }
                TableFields {
                    key: if fields.key.is_empty() {
                        schema_key()
                    } else {
                        fields.key
                    },
                    must,
                    update,
                }
            }
        };
        let column_positions: HashMap<String, usize> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        if column_positions.is_empty() {
            anyhow::bail!(
                "canal-client table '{}': source schema has no columns",
                camel
            );
        }
        if self.default_table.is_empty() {
            self.default_table = identifier.clone();
        }
        self.tables.insert(
            identifier,
            TableState {
                db_name: db_name.to_string(),
                table_name: table_name.to_string(),
                fields,
                tz: self.tz,
                column_positions,
                pending: None,
            },
        );
        Ok(())
    }

    /// The resolved server timezone (diagnostics).
    pub fn server_tz(&self) -> &ServerTz {
        &self.tz
    }

    /// The effective canal-client configuration (diagnostics).
    pub fn config(&self) -> &CanalClientConfig {
        &self.config
    }

    /// The effective field mapping of the default table (diagnostics).
    pub fn fields(&self) -> &TableFields {
        &self
            .tables
            .get(&self.default_table)
            .map(|s| &s.fields)
            .expect("default table state exists")
    }

    /// Number of registered per-table states (diagnostics).
    pub fn registered_tables(&self) -> usize {
        self.tables.len()
    }

    /// Whether this encoder was built from explicit `canal-client.columns`
    /// (single-table authoritative) instead of schema registration.
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Pick the state serving this row: its origin table when carried,
    /// the static default table otherwise.
    fn state_for(&mut self, row: &Row) -> anyhow::Result<&mut TableState> {
        // Resolve the key first, then borrow once (the bail paths return
        // before any borrow is live).
        let key: String = match row.origin_table.as_deref() {
            None => {
                if self.default_table.is_empty() {
                    anyhow::bail!(
                        "canal-client automatic column mapping: row arrived before any \
                         initial schema event — the source must emit the table schema first \
                         (MySQL-CDC does; or configure canal-client.columns explicitly)"
                    );
                }
                self.default_table.clone()
            }
            Some(origin) => {
                if self.tables.contains_key(origin) {
                    origin.to_string()
                } else if self.explicit {
                    // Case-insensitive: the static config may spell the
                    // identity differently than the binlog does.
                    if origin.eq_ignore_ascii_case(&self.default_table) {
                        self.default_table.clone()
                    } else {
                        anyhow::bail!(
                            "canal-client explicit config targets table '{}' but a row arrived \
                             from '{}' — multi-table sources need the schema-driven mode (drop \
                             canal-client.columns and let the initial-schema events map them)",
                            self.default_table,
                            origin
                        );
                    }
                } else {
                    anyhow::bail!(
                        "canal-client automatic column mapping: row from table '{}' arrived \
                         before its initial schema event",
                        origin
                    );
                }
            }
        };
        Ok(self
            .tables
            .get_mut(&key)
            .expect("resolved table state exists"))
    }

    /// Encode one row into 0..2 messages of its own table. Zero messages
    /// = filtered (update without any configured column change, or a
    /// before-image held for pairing).
    pub fn encode(&mut self, row: &Row) -> anyhow::Result<Vec<CanalMessage>> {
        Ok(self.state_for(row)?.encode(row))
    }

    /// Flush every held before-image (job close / bounded end).
    pub fn flush(&mut self) -> Vec<CanalMessage> {
        self.tables
            .values_mut()
            .flat_map(TableState::flush)
            .collect()
    }

    /// Emit held before-images as real deletes once the pairing window
    /// expired (called from each sink flush cycle).
    pub fn expire_pending(&mut self) -> Vec<CanalMessage> {
        self.tables
            .values_mut()
            .flat_map(TableState::expire_pending)
            .collect()
    }
}

/// Split a `database.table` identifier at the last dot; an identifier
/// without a dot degrades to an empty database.
fn split_table_identifier(identifier: &str) -> (&str, &str) {
    match identifier.rsplit_once('.') {
        Some((db, table)) => (db, table),
        None => ("", identifier),
    }
}

/// Deserialize a canal-client message into rows (source side): fields are
/// pulled from `data` by schema column name; delete → RowKind::Delete.
pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let event_type = value
        .get("eventType")
        .and_then(|v| v.as_str())
        .unwrap_or("insert");
    let kind = match event_type {
        "update" => RowKind::UpdateAfter,
        "delete" => RowKind::Delete,
        _ => RowKind::Insert,
    };
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let mut row = Row::new(kind, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        let field = data
            .get(&col.name)
            .map(json_value_to_field)
            .unwrap_or(Field::Null);
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
    let (db_name, table_name) = row
        .origin_table
        .as_deref()
        .map(split_table_identifier)
        .unwrap_or(("", ""));
    message.insert("dbName".to_string(), Value::String(db_name.to_lowercase()));
    message.insert(
        "tableName".to_string(),
        Value::String(table_name.to_lowercase()),
    );
    let (event_type, _) = match row.kind {
        RowKind::Insert => ("insert", ()),
        RowKind::UpdateAfter => ("update", ()),
        RowKind::UpdateBefore => ("update", ()),
        RowKind::Delete => ("delete", ()),
    };
    message.insert(
        "eventType".to_string(),
        Value::String(event_type.to_string()),
    );
    let mut data = Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        data.insert(
            col.name.clone(),
            convert_field(
                &ServerTz::default(),
                row.fields.get(i).unwrap_or(&Field::Null),
            ),
        );
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
    fn test_string_conversion_utc() {
        let tz = ServerTz::Utc;
        // Strict date → epoch millis (2024-05-06 07:08:09 UTC).
        let v = convert_string(&tz, "2024-05-06 07:08:09");
        assert_eq!(v, Value::Number(Number::from(1714979289000i64)));
        // Non-strict date stays a string.
        assert_eq!(
            convert_string(&tz, "2024-05-06"),
            Value::String("2024-05-06".into())
        );
        assert_eq!(
            convert_string(&tz, "2024-13-40 00:00:00"),
            Value::String("2024-13-40 00:00:00".into())
        );
        // Numbers.
        assert_eq!(
            convert_string(&tz, "1001"),
            Value::Number(Number::from(1001i64))
        );
        assert_eq!(convert_string(&tz, "0123"), Value::String("0123".into()));
    }

    #[test]
    fn test_string_conversion_respects_server_time_zone() {
        // Same wall-clock time, different zones → different millis.
        let utc = convert_string(&ServerTz::Utc, "2024-05-06 07:08:09");
        let utc_millis = utc.as_i64().expect("number");
        // +08:00: 8 hours earlier in absolute time.
        let cst = convert_string(&ServerTz::parse("+08:00"), "2024-05-06 07:08:09");
        assert_eq!(cst.as_i64().unwrap(), utc_millis - 8 * 3600 * 1000);
        // IANA name agrees with the fixed offset.
        let shanghai = convert_string(&ServerTz::parse("Asia/Shanghai"), "2024-05-06 07:08:09");
        assert_eq!(shanghai.as_i64().unwrap(), utc_millis - 8 * 3600 * 1000);
        // Negative offset: -05:00 → 5 hours later in absolute time.
        let ny = convert_string(&ServerTz::parse("-05:00"), "2024-05-06 07:08:09");
        assert_eq!(ny.as_i64().unwrap(), utc_millis + 5 * 3600 * 1000);
    }

    #[test]
    fn test_server_tz_parse_forms() {
        assert!(matches!(ServerTz::parse(""), ServerTz::Local));
        assert!(matches!(ServerTz::parse("local"), ServerTz::Local));
        assert!(matches!(ServerTz::parse("UTC"), ServerTz::Utc));
        assert!(matches!(ServerTz::parse("+08:00"), ServerTz::Fixed(_)));
        assert!(matches!(
            ServerTz::parse("Asia/Shanghai"),
            ServerTz::Named(_)
        ));
        // Bogus values fall back to the local zone.
        assert!(matches!(ServerTz::parse("not-a-zone"), ServerTz::Local));
        // Default is the server-local zone (Java behavior).
        assert!(matches!(ServerTz::default(), ServerTz::Local));
    }

    #[test]
    fn test_datetime_field_uses_configured_zone() {
        let dt = NaiveDateTime::parse_from_str("2024-05-06 07:08:09", "%Y-%m-%d %H:%M:%S").unwrap();
        let utc = convert_field(&ServerTz::Utc, &Field::DateTime(dt));
        let cst = convert_field(&ServerTz::parse("+08:00"), &Field::DateTime(dt));
        assert_eq!(
            cst.as_i64().unwrap(),
            utc.as_i64().unwrap() - 8 * 3600 * 1000
        );
    }

    fn fields_config() -> CanalClientConfig {
        let tables: HashMap<String, TableFields> = serde_json::from_str(
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
            server_time_zone: "UTC".to_string(),
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
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 1001, "张三", 1))
            .unwrap();
        assert_eq!(messages.len(), 1);
        let (key, payload) = (&messages[0].key, &messages[0].payload);
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
        let json: Value = serde_json::from_str(&messages[0].payload).unwrap();
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
        encoder
            .encode(&row_of(RowKind::Delete, 1001, "same", 0))
            .unwrap();
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 1001, "same", 0))
            .unwrap();
        assert!(
            messages.is_empty(),
            "update without configured changes must be filtered"
        );
    }

    #[test]
    fn test_encoder_real_delete_flushes_before_unrelated_insert() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        encoder
            .encode(&row_of(RowKind::Delete, 7, "gone", 0))
            .unwrap();
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 1001, "new", 1))
            .unwrap();
        assert_eq!(messages.len(), 2);
        let delete: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(delete["eventType"], "delete");
        assert_eq!(delete["data"]["id"], 7);
        // Delete data carries must fields only (no status).
        assert!(delete["data"].get("status").is_none());
        let insert: Value = serde_json::from_str(&messages[1].payload).unwrap();
        assert_eq!(insert["eventType"], "insert");
    }

    #[test]
    fn test_encoder_flush_emits_trailing_delete() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        encoder
            .encode(&row_of(RowKind::Delete, 9, "tail", 0))
            .unwrap();
        let flushed = encoder.flush();
        assert_eq!(flushed.len(), 1);
        let json: Value = serde_json::from_str(&flushed[0].payload).unwrap();
        assert_eq!(json["eventType"], "delete");
        assert_eq!(json["data"]["id"], 9);
    }

    fn auto_schema() -> seatunnel_api::TableSchema {
        seatunnel_api::TableSchema::new(
            "neworiental_user.l_class_student",
            vec![
                seatunnel_api::ColumnDef::new("id", seatunnel_api::ColumnType::Int64).primary_key(),
                seatunnel_api::ColumnDef::new("name", seatunnel_api::ColumnType::String),
                seatunnel_api::ColumnDef::new("status", seatunnel_api::ColumnType::Int32),
            ],
        )
    }

    fn auto_config() -> CanalClientConfig {
        CanalClientConfig {
            database_name: "neworiental_user".into(),
            table_name: "l_class_student".into(),
            columns: Vec::new(), // automatic: derived from the schema
            tables: HashMap::new(),
            server_time_zone: "UTC".to_string(),
        }
    }

    #[test]
    fn test_from_schema_auto_maps_all_columns_identity() {
        let mut encoder = CanalClientEncoder::from_schema(auto_config(), &auto_schema()).unwrap();
        // All columns in `must`, no update filter, PK as the key column.
        assert_eq!(encoder.fields().key, "id");
        assert_eq!(encoder.fields().must.len(), 3);
        assert!(encoder.fields().update.is_empty());
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 7, "张三", 2))
            .unwrap();
        let json: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(json["data"]["id"], 7);
        assert_eq!(json["data"]["name"], "张三");
        assert_eq!(json["data"]["status"], 2);
    }

    #[test]
    fn test_from_schema_delta_entry_merges_over_identity() {
        // The explicit entry defines only the DIFFERENCES from the
        // identity baseline: `status` moves to changed-only, unmentioned
        // columns stay identity always-present, empty `key` falls back
        // to the schema primary key.
        let mut config = auto_config();
        config.tables.insert(
            "lClassStudent".into(),
            TableFields {
                key: String::new(),
                must: HashMap::new(),
                update: [("status".to_string(), "status".to_string())].into(),
            },
        );
        let mut encoder = CanalClientEncoder::from_schema(config, &auto_schema()).unwrap();
        assert_eq!(encoder.fields().key, "id");
        assert_eq!(encoder.fields().must.len(), 2, "id + name stay in must");
        assert_eq!(encoder.fields().update.len(), 1);

        // INSERT carries every column (changed-only columns included).
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 7, "张三", 2))
            .unwrap();
        let json: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(json["data"]["id"], 7);
        assert_eq!(json["data"]["name"], "张三");
        assert_eq!(json["data"]["status"], 2);

        // UPDATE touching only `name` (identity must): still emitted,
        // `data` carries the must columns; unchanged `status` stays out
        // of data but appears in oldData.
        encoder
            .encode(&row_of(RowKind::Delete, 7, "李四", 2))
            .unwrap();
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 7, "张三", 2))
            .unwrap();
        let json: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(json["eventType"], "update");
        assert_eq!(json["data"]["name"], "张三");
        assert!(json["data"].get("status").is_none());
        assert_eq!(json["oldData"]["status"], 2);

        // UPDATE touching only `status` (changed-only column): emitted
        // with status in data.
        encoder
            .encode(&row_of(RowKind::Delete, 7, "张三", 2))
            .unwrap();
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 7, "张三", 5))
            .unwrap();
        let json: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(json["eventType"], "update");
        assert_eq!(json["data"]["status"], 5);
        assert_eq!(json["oldData"]["status"], 2);
    }

    #[test]
    fn test_from_schema_delta_entry_renames_field() {
        // A `must` rename only changes the target field name; the column
        // stays always-present and every other column stays identity.
        let mut config = auto_config();
        config.tables.insert(
            "lClassStudent".into(),
            TableFields {
                key: "name".to_string(),
                must: [("name".to_string(), "studentName".to_string())].into(),
                update: HashMap::new(),
            },
        );
        let mut encoder = CanalClientEncoder::from_schema(config, &auto_schema()).unwrap();
        assert_eq!(encoder.fields().key, "name");
        let messages = encoder
            .encode(&row_of(RowKind::Insert, 7, "张三", 2))
            .unwrap();
        // Partition key is the renamed column's VALUE.
        assert_eq!(messages[0].key, "张三");
        let json: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(json["data"]["studentName"], "张三");
        assert!(json["data"].get("name").is_none(), "renamed away");
        assert_eq!(json["data"]["id"], 7, "unmentioned column identity");
        assert_eq!(json["data"]["status"], 2);
    }

    #[test]
    fn test_from_schema_delta_entry_rejects_unknown_columns() {
        let mut config = auto_config();
        config.tables.insert(
            "lClassStudent".into(),
            TableFields {
                key: String::new(),
                must: [("bogus".to_string(), "x".to_string())].into(),
                update: HashMap::new(),
            },
        );
        let err = CanalClientEncoder::from_schema(config, &auto_schema())
            .unwrap_err()
            .to_string();
        assert!(err.contains("bogus"), "error names the column: {err}");

        let mut config = auto_config();
        config.tables.insert(
            "lClassStudent".into(),
            TableFields {
                key: "nope".to_string(),
                must: HashMap::new(),
                update: HashMap::new(),
            },
        );
        let err = CanalClientEncoder::from_schema(config, &auto_schema())
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "error names the key: {err}");
    }

    #[test]
    fn test_from_schema_no_pk_falls_back_to_first_column() {
        let schema = seatunnel_api::TableSchema::new(
            "db.t",
            vec![
                seatunnel_api::ColumnDef::new("code", seatunnel_api::ColumnType::String),
                seatunnel_api::ColumnDef::new("value", seatunnel_api::ColumnType::String),
            ],
        );
        let config = CanalClientConfig {
            database_name: "db".into(),
            table_name: "t".into(),
            columns: Vec::new(),
            tables: HashMap::new(),
            server_time_zone: "UTC".to_string(),
        };
        let encoder = CanalClientEncoder::from_schema(config, &schema).unwrap();
        assert_eq!(encoder.fields().key, "code");
    }

    #[test]
    fn test_from_schema_empty_schema_errors() {
        let schema = seatunnel_api::TableSchema::new("db.t", Vec::new());
        assert!(CanalClientEncoder::from_schema(auto_config(), &schema).is_err());
    }

    fn second_auto_schema() -> seatunnel_api::TableSchema {
        seatunnel_api::TableSchema::new(
            "neworiental_user.l_class_course",
            vec![
                seatunnel_api::ColumnDef::new("cid", seatunnel_api::ColumnType::Int64)
                    .primary_key(),
                seatunnel_api::ColumnDef::new("title", seatunnel_api::ColumnType::String),
            ],
        )
    }

    /// `l_class_student` row carrying its origin identity.
    fn origin_row_of(kind: RowKind, id: i64, name: &str, status: i64) -> Row {
        let mut row = row_of(kind, id, name, status);
        row.origin_table = Some("neworiental_user.l_class_student".to_string());
        row
    }

    /// `l_class_course` row (different column layout) with its origin.
    fn course_row_of(kind: RowKind, cid: i64, title: &str) -> Row {
        let mut row = Row::new(kind, 2);
        row.set(0, Field::Int64(cid));
        row.set(1, Field::String(title.into()));
        row.origin_table = Some("neworiental_user.l_class_course".to_string());
        row
    }

    #[test]
    fn test_multi_table_auto_registers_and_stamps_identity() {
        let mut encoder = CanalClientEncoder::new_auto(auto_config());
        encoder.register_schema(&auto_schema()).unwrap();
        encoder.register_schema(&second_auto_schema()).unwrap();
        assert_eq!(encoder.registered_tables(), 2);
        // Re-registration (event replay) is idempotent.
        encoder.register_schema(&auto_schema()).unwrap();
        assert_eq!(encoder.registered_tables(), 2);

        // Each row encodes against ITS table's schema and identity.
        let messages = encoder
            .encode(&origin_row_of(RowKind::Insert, 1001, "张三", 1))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].table, "neworiental_user.l_class_student");
        let student: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(student["dbName"], "neworiental_user");
        assert_eq!(student["tableName"], "l_class_student");
        assert_eq!(student["data"]["name"], "张三");

        let messages = encoder
            .encode(&course_row_of(RowKind::Insert, 7, "math"))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].table, "neworiental_user.l_class_course");
        let course: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(course["tableName"], "l_class_course");
        assert_eq!(course["data"]["title"], "math");

        // UPDATE pairs within the same table only.
        let held = encoder
            .encode(&origin_row_of(RowKind::Delete, 1001, "李四", 0))
            .unwrap();
        assert!(held.is_empty());
        let messages = encoder
            .encode(&origin_row_of(RowKind::Insert, 1001, "张三", 0))
            .unwrap();
        assert_eq!(messages.len(), 1);
        let update: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(update["tableName"], "l_class_student");
        assert_eq!(update["eventType"], "update");
        assert_eq!(update["oldData"]["name"], "李四");
    }

    #[test]
    fn test_multi_table_same_key_rows_never_cross_pair() {
        let mut encoder = CanalClientEncoder::new_auto(auto_config());
        encoder.register_schema(&auto_schema()).unwrap();
        encoder.register_schema(&second_auto_schema()).unwrap();

        // student delete(id=1) held; a course insert with the SAME key
        // value must NOT pair with it — it stays a plain insert of its
        // own table.
        encoder
            .encode(&origin_row_of(RowKind::Delete, 1, "gone", 0))
            .unwrap();
        let messages = encoder
            .encode(&course_row_of(RowKind::Insert, 1, "math"))
            .unwrap();
        assert_eq!(messages.len(), 1);
        let course: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(course["tableName"], "l_class_course");
        assert_eq!(
            course["eventType"], "insert",
            "same key on another table must not pair into an update"
        );

        // The held student before-image still pairs with its own table's
        // after-image afterwards.
        let messages = encoder
            .encode(&origin_row_of(RowKind::Insert, 1, "renamed", 0))
            .unwrap();
        assert_eq!(messages.len(), 1);
        let update: Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(update["eventType"], "update");
        assert_eq!(update["tableName"], "l_class_student");
    }

    #[test]
    fn test_multi_table_flush_and_expire_carry_each_table() {
        let mut encoder = CanalClientEncoder::new_auto(auto_config());
        encoder.register_schema(&auto_schema()).unwrap();
        encoder.register_schema(&second_auto_schema()).unwrap();

        // Leave one unpaired before-image per table.
        encoder
            .encode(&origin_row_of(RowKind::Delete, 9, "tail-student", 0))
            .unwrap();
        encoder
            .encode(&course_row_of(RowKind::Delete, 3, "tail-course"))
            .unwrap();

        let flushed = encoder.flush();
        assert_eq!(flushed.len(), 2, "one trailing delete per table");
        let tables: Vec<&str> = flushed.iter().map(|m| m.table.as_str()).collect();
        assert!(tables.contains(&"neworiental_user.l_class_student"));
        assert!(tables.contains(&"neworiental_user.l_class_course"));
        for message in &flushed {
            let json: Value = serde_json::from_str(&message.payload).unwrap();
            assert_eq!(json["eventType"], "delete");
        }
    }

    #[test]
    fn test_explicit_encoder_rejects_foreign_table_row() {
        let mut encoder = CanalClientEncoder::new(fields_config()).unwrap();
        // Without origin the row belongs to the configured table.
        assert!(encoder.encode(&row_of(RowKind::Insert, 1, "a", 0)).is_ok());
        // With a foreign origin it must fail loudly instead of being
        // stamped with the wrong identity.
        let mut foreign = row_of(RowKind::Insert, 1, "a", 0);
        foreign.origin_table = Some("other_db.other_table".to_string());
        let err = encoder.encode(&foreign).unwrap_err().to_string();
        assert!(
            err.contains("other_db.other_table"),
            "error names the foreign table: {err}"
        );
        // Case differences are tolerated (binlog vs config spelling).
        let mut casing = row_of(RowKind::Insert, 1, "a", 0);
        casing.origin_table = Some("NEWORIENTAL_DATA_RECOMMAND.L_CLASS_STUDENT".to_string());
        assert!(encoder.encode(&casing).is_ok());
    }

    #[test]
    fn test_auto_row_from_unregistered_table_errors() {
        let mut encoder = CanalClientEncoder::new_auto(auto_config());
        let err = encoder
            .encode(&origin_row_of(RowKind::Insert, 1, "a", 0))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("neworiental_user.l_class_student"),
            "error names the table: {err}"
        );
    }

    #[test]
    fn test_stateless_serialize_stamps_origin() {
        let mut row = row_of(RowKind::Insert, 1, "a", 0);
        row.origin_table = Some("MyDb.MyTable".to_string());
        let bytes = serialize(&auto_schema(), &row).unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["dbName"], "mydb");
        assert_eq!(json["tableName"], "mytable");
    }
}
