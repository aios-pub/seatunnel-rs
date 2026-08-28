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

use seatunnel_api::{schema::TableSchema, source::source_split::SourceSplit};
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Watermark {
    #[default]
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
            id: format!(
                "incremental-{}-{}-{}",
                database,
                table,
                uuid::Uuid::new_v4()
            ),
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

pub use seatunnel_api::SchemaChangeEvent;

/// Result of diffing two column lists: the schema changes (in application
/// order) that transform `old` into `new`.
///
/// Rename detection heuristic: a diff that drops exactly one column and adds
/// exactly one column with an identical definition is interpreted as a rename
/// (metadata polling cannot distinguish rename from drop+add by itself).
pub fn diff_columns(
    table: &str,
    old: &[seatunnel_api::ColumnDef],
    new: &[seatunnel_api::ColumnDef],
) -> Vec<SchemaChangeEvent> {
    use seatunnel_api::{ColumnDef, SchemaChange};

    let old_by_name: std::collections::HashMap<&str, &ColumnDef> =
        old.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_by_name: std::collections::HashMap<&str, &ColumnDef> =
        new.iter().map(|c| (c.name.as_str(), c)).collect();

    let added: Vec<&ColumnDef> = new
        .iter()
        .filter(|c| !old_by_name.contains_key(c.name.as_str()))
        .collect();
    let dropped: Vec<&str> = old
        .iter()
        .filter(|c| !new_by_name.contains_key(c.name.as_str()))
        .map(|c| c.name.as_str())
        .collect();
    let modified: Vec<(&ColumnDef, &ColumnDef)> = new
        .iter()
        .filter_map(|c| {
            let prev = *old_by_name.get(c.name.as_str())?;
            (prev.column_type != c.column_type || prev.nullable != c.nullable).then_some((prev, c))
        })
        .collect();

    // Rename heuristic: single drop + single add with equal definitions.
    if added.len() == 1
        && dropped.len() == 1
        && modified.is_empty()
        && added[0].column_type == old_by_name[dropped[0]].column_type
        && added[0].nullable == old_by_name[dropped[0]].nullable
    {
        return vec![SchemaChangeEvent::new(
            table,
            vec![SchemaChange::rename_column(
                dropped[0],
                added[0].name.clone(),
            )],
        )];
    }

    // Positions in the NEW layout so positional sinks can map by ordinal.
    let position_of = |name: &str| -> Option<usize> { new.iter().position(|c| c.name == name) };
    let mut changes: Vec<SchemaChange> = Vec::new();
    for col in added {
        let pos = position_of(&col.name);
        changes.push(match pos {
            Some(p) => SchemaChange::add_column_at(col.clone(), p),
            None => SchemaChange::add_column(col.clone()),
        });
    }
    for name in dropped {
        let pos = old.iter().position(|c| c.name == name);
        changes.push(SchemaChange::DropColumn {
            column_name: name.to_string(),
            position: pos,
        });
    }
    for (prev, col) in modified {
        let pos = position_of(&col.name);
        changes.push(match pos {
            Some(p) => SchemaChange::modify_column_at(col.clone(), p),
            None => SchemaChange::modify_column(col.clone()),
        });
        // Keep nullability/type metadata coherent for consumers comparing defs.
        let _ = prev;
    }
    if changes.is_empty() {
        return Vec::new();
    }
    vec![SchemaChangeEvent::new(table, changes)]
}

// ---------------------------------------------------------------------------
// Table selection (official option set)
// ---------------------------------------------------------------------------

/// Resolved database/table selection from the official option set:
/// `database-names` + `database-pattern`, `table-names` (exact
/// `db.table` refs) + `table-pattern` (regex over `db.table`), with the
/// legacy single `database-name`/`table-name` (trailing `%` wildcard)
/// folded in.
#[derive(Debug, Clone, Default)]
pub struct TableSelector {
    split_columns: std::collections::HashMap<String, String>,
    databases: Vec<String>,
    database_pattern: Option<regex::Regex>,
    tables: Vec<(String, String)>,
    table_patterns: Vec<regex::Regex>,
}

impl TableSelector {
    fn compile(pattern: &str) -> Option<regex::Regex> {
        regex::Regex::new(pattern)
            .map_err(|e| tracing::warn!("invalid table-selection regex '{}': {}", pattern, e))
            .ok()
    }

    /// Legacy single names with a trailing `%` wildcard become regexes;
    /// empty strings produce an empty selector (official options replace
    /// the legacy forms).
    pub fn from_legacy(database: &str, table: &str) -> Self {
        let mut selector = TableSelector::default();
        if database.is_empty() && table.is_empty() {
            return selector;
        }
        if let Some(prefix) = database.strip_suffix('%') {
            selector.database_pattern = Self::compile(&format!("^{}.*$", regex::escape(prefix)));
        } else {
            selector.databases.push(database.to_string());
        }
        if let Some(prefix) = table.strip_suffix('%') {
            // Legacy wildcards name the bare table; official table regexes
            // match the qualified `db.table`. Match only the table segment
            // (the database gate applies separately).
            selector.table_patterns.push(
                Self::compile(&format!("^.*\\.{}.*$", regex::escape(prefix)))
                    .expect("escaped regex"),
            );
        } else {
            selector
                .tables
                .push((database.to_string(), table.to_string()));
        }
        selector
    }

    fn matches_database(&self, database: &str) -> bool {
        self.databases.iter().any(|d| d == database)
            || self
                .database_pattern
                .as_ref()
                .is_some_and(|re| re.is_match(database))
    }

    /// Exact `db.table` refs match their own pair; regexes are matched
    /// against the fully-qualified `db.table` string (official semantics).
    ///
    /// Table-level selection (exact pairs or `table-pattern`) decides on
    /// its own — the refs/patterns already carry the database part, so a
    /// `table-names`-only config captures without `database-names`. When a
    /// database list IS also given it only narrows table-level selection
    /// further (official combined semantics). With no table-level
    /// selectors, `database-names`/`database-pattern` alone subscribe to
    /// every table of the matching databases.
    pub fn matches(&self, database: &str, table: &str) -> bool {
        let table_selected = self.has_exact() || !self.table_patterns.is_empty();
        if table_selected {
            let qualified = format!("{}.{}", database, table);
            let hit = self.tables.iter().any(|(d, t)| d == database && t == table)
                || self.table_patterns.iter().any(|re| re.is_match(&qualified));
            return hit && (self.databases.is_empty() || self.matches_database(database));
        }
        self.matches_database(database)
    }

    /// Database names in the selection (diagnostics).
    pub fn databases(&self) -> &[String] {
        &self.databases
    }

    /// True when at least one exact pair is registered.
    pub fn has_exact(&self) -> bool {
        !self.tables.is_empty()
    }

    /// Split column override for `db.table` (from `table-names-config`).
    pub fn split_column_for(&self, _database: &str, _table: &str) -> Option<&str> {
        self.split_columns
            .get(&format!("{}.{}", _database, _table))
            .map(String::as_str)
    }
}

/// Assemble the [`TableSelector`] from the official option set, folding in
/// the legacy single-name forms.
pub fn build_table_selector(
    config: &seatunnel_connector_common::ConnectorConfig,
    legacy_db: &str,
    legacy_table: &str,
) -> TableSelector {
    // Official options REPLACE the legacy single-name forms; legacy
    // selection only applies when its official counterpart is absent.
    let has_official_databases = !config
        .get_string("database-names", &config.get_string("database_names", ""))
        .is_empty()
        || !config
            .get_string(
                "database-pattern",
                &config.get_string("database_pattern", ""),
            )
            .is_empty();
    let has_official_tables = !config
        .get_string("table-names", &config.get_string("table_names", ""))
        .is_empty()
        || !config
            .get_string("table-pattern", &config.get_string("table_pattern", ""))
            .is_empty();
    let mut selector = TableSelector::from_legacy(
        // The legacy database only applies when NEITHER official database
        // NOR table selection is present: official `table-names` refs and
        // `table-pattern` regexes are fully qualified (`db.table`), so
        // folding a legacy default database name on top would gate out
        // every database other than that default.
        if has_official_databases || has_official_tables {
            ""
        } else {
            legacy_db
        },
        if has_official_tables {
            ""
        } else {
            legacy_table
        },
    );

    // database-names: comma list of exact names (arrays arrive comma-joined).
    let databases = config.get_string("database-names", &config.get_string("database_names", ""));
    for db in databases
        .split(',')
        .map(str::trim)
        .filter(|d| !d.is_empty())
    {
        if !selector.databases.contains(&db.to_string()) {
            selector.databases.push(db.to_string());
        }
    }
    // database-pattern: regex.
    let db_pattern = config.get_string(
        "database-pattern",
        &config.get_string("database_pattern", ""),
    );
    if !db_pattern.is_empty() {
        if let Some(re) = TableSelector::compile(&format!("^(?:{})$", db_pattern)) {
            selector.database_pattern = Some(re);
        }
    }
    // table-names: exact `db.table` entries (comma list).
    let tables = config.get_string("table-names", &config.get_string("table_names", ""));
    for qualified in tables.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        if let Some((db, table)) = qualified.rsplit_once('.') {
            selector.tables.push((db.to_string(), table.to_string()));
        }
    }
    // table-pattern: regex over `db.table`.
    let table_pattern = config.get_string("table-pattern", &config.get_string("table_pattern", ""));
    if !table_pattern.is_empty() {
        if let Some(re) = TableSelector::compile(&table_pattern) {
            selector.table_patterns.push(re);
        }
    }
    // table-names-config: per-table primaryKeys / snapshotSplitColumn.
    let config_list = config.get_string(
        "table-names-config",
        &config.get_string("table_names_config", ""),
    );
    if !config_list.is_empty() {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&config_list).unwrap_or_default();
        for entry in parsed {
            let Some(table) = entry.get("table").and_then(|t| t.as_str()) else {
                continue;
            };
            if let Some(split_col) = entry.get("snapshotSplitColumn").and_then(|c| c.as_str()) {
                selector
                    .split_columns
                    .insert(table.to_string(), split_col.to_string());
            }
        }
    }
    selector
}

// ---------------------------------------------------------------------------
// Schema evolution machinery
// ---------------------------------------------------------------------------

/// Schema-evolution configuration shared by CDC connectors.
///
/// Java counterpart: `SourceOptions.SCHEMA_CHANGES_ENABLED` (default false)
/// and the per-connector DDL resolvers.
#[derive(Debug, Clone)]
pub struct SchemaEvolutionConfig {
    pub enabled: bool,
    /// information_schema polling interval for connectors without a DDL
    /// stream (TiDB / Postgres).
    pub poll_interval_ms: u64,
    /// Only emit changes for these columns (Java `schema-changes.include`).
    pub include: Vec<String>,
    /// Never emit changes for these columns (Java `schema-changes.exclude`).
    pub exclude: Vec<String>,
}

impl Default for SchemaEvolutionConfig {
    fn default() -> Self {
        SchemaEvolutionConfig {
            enabled: false,
            poll_interval_ms: 10_000,
            include: Vec::new(),
            exclude: Vec::new(),
        }
    }
}

impl SchemaEvolutionConfig {
    fn split_list(s: &str) -> Vec<String> {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }

    pub fn from_config(config: &seatunnel_connector_common::ConnectorConfig) -> Self {
        SchemaEvolutionConfig {
            enabled: config.get_bool(
                "schema-evolution.enabled",
                config.get_bool("schema_evolution.enabled", false),
            ),
            poll_interval_ms: config
                .get_int(
                    "schema-evolution.poll-interval-ms",
                    config.get_int("schema_evolution.poll_interval_ms", 10_000),
                )
                .max(100) as u64,
            include: Self::split_list(&config.get_string(
                "schema-evolution.include",
                &config.get_string("schema_changes.include", ""),
            )),
            exclude: Self::split_list(&config.get_string(
                "schema-evolution.exclude",
                &config.get_string("schema_changes.exclude", ""),
            )),
        }
    }

    /// Whether a change on `column` passes the include/exclude filters.
    fn accepts_column(&self, column: &str) -> bool {
        if !self.include.is_empty() && !self.include.iter().any(|c| c == column) {
            return false;
        }
        !self.exclude.iter().any(|c| c == column)
    }
}

/// Tracks a table's columns and detects changes either from captured DDL
/// statements (`observe_ddl`) or by diffing freshly-fetched column lists
/// on an interval (`poll`).
pub struct SchemaWatcher {
    pub table_id: String,
    columns: Vec<seatunnel_api::ColumnDef>,
    enabled: bool,
    filters: SchemaEvolutionConfig,
    interval: std::time::Duration,
    last_check: std::time::Instant,
    pending: std::collections::VecDeque<SchemaChangeEvent>,
}

impl SchemaWatcher {
    pub fn new(table_id: impl Into<String>, config: &SchemaEvolutionConfig) -> Self {
        SchemaWatcher {
            table_id: table_id.into(),
            columns: Vec::new(),
            enabled: config.enabled,
            filters: config.clone(),
            interval: std::time::Duration::from_millis(config.poll_interval_ms),
            last_check: std::time::Instant::now(),
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Set the baseline column list (no events emitted).
    pub fn prime(&mut self, columns: Vec<seatunnel_api::ColumnDef>) {
        self.columns = columns;
    }

    /// Queue an initial-schema event carrying the primed column layout.
    /// Called once after [`prime`](Self::prime): the event flows through
    /// the stream before the table's first row, letting schema-driven
    /// sinks configure themselves without static column config.
    pub fn queue_initial(&mut self) {
        if self.columns.is_empty() {
            return;
        }
        let schema = seatunnel_api::TableSchema::new(self.table_id.clone(), self.columns.clone());
        self.pending
            .push_back(SchemaChangeEvent::initial_schema(schema));
    }

    pub fn columns(&self) -> &[seatunnel_api::ColumnDef] {
        &self.columns
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Apply a captured DDL statement to the baseline and queue the
    /// resulting event (MySQL-style binlog query events).
    pub fn observe_ddl(&mut self, ddl: &str) {
        if !self.enabled {
            return;
        }
        if let Some(changes) = parse_alter_table(ddl) {
            let changes: Vec<seatunnel_api::SchemaChange> = changes
                .into_iter()
                .filter(|c| self.filters.accepts_column(c.column_name()))
                .collect();
            if changes.is_empty() {
                return;
            }
            // Attach positions from the baseline so positional sinks can
            // map source column names to their own fN scheme.
            let changes: Vec<seatunnel_api::SchemaChange> = changes
                .into_iter()
                .map(|change| match &change {
                    seatunnel_api::SchemaChange::AddColumn { .. } => {
                        let pos = Some(self.columns.len());
                        set_position(change, pos)
                    }
                    other => {
                        let name = other.column_name().to_string();
                        let pos = self.columns.iter().position(|c| c.name == name);
                        set_position(change, pos)
                    }
                })
                .collect();
            let event =
                SchemaChangeEvent::new(self.table_id.clone(), changes).with_statement(ddl.trim());
            if !self.columns.is_empty() {
                let mut schema =
                    seatunnel_api::TableSchema::new(self.table_id.clone(), self.columns.clone());
                if schema.apply_schema_change_event(&event).is_ok() {
                    self.columns = schema.columns;
                }
            }
            tracing::info!(
                "schema change on {}: {} change(s) from DDL",
                self.table_id,
                event.changes.len()
            );
            self.pending.push_back(event);
        }
    }

    /// Diff freshly-fetched columns against the baseline on the configured
    /// interval. `fetch` is connector-specific (information_schema query).
    pub async fn poll<F, Fut>(&mut self, fetch: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Vec<seatunnel_api::ColumnDef>>>,
    {
        if !self.enabled || self.columns.is_empty() {
            return Ok(());
        }
        if self.last_check.elapsed() < self.interval {
            return Ok(());
        }
        self.last_check = std::time::Instant::now();
        let fresh = fetch().await?;
        if fresh.is_empty() {
            return Ok(());
        }
        for mut event in diff_columns(&self.table_id, &self.columns, &fresh) {
            event
                .changes
                .retain(|c| self.filters.accepts_column(c.column_name()));
            if event.changes.is_empty() {
                continue;
            }
            tracing::info!(
                "schema change on {}: {} change(s) detected by poll",
                event.table,
                event.changes.len()
            );
            self.columns = fresh.clone();
            self.pending.push_back(event);
        }
        Ok(())
    }

    /// Take the next pending schema-change event.
    pub fn take_pending(&mut self) -> Option<SchemaChangeEvent> {
        self.pending.pop_front()
    }
}

fn set_position(
    change: seatunnel_api::SchemaChange,
    position: Option<usize>,
) -> seatunnel_api::SchemaChange {
    use seatunnel_api::SchemaChange;
    match (change, position) {
        (SchemaChange::AddColumn { column, .. }, pos) => match pos {
            Some(p) => SchemaChange::add_column_at(column, p),
            None => SchemaChange::add_column(column),
        },
        (SchemaChange::DropColumn { column_name, .. }, pos) => SchemaChange::DropColumn {
            column_name,
            position: pos,
        },
        (
            SchemaChange::RenameColumn {
                old_name, new_name, ..
            },
            pos,
        ) => SchemaChange::RenameColumn {
            old_name,
            new_name,
            position: pos,
        },
        (SchemaChange::ModifyColumn { column, .. }, pos) => match pos {
            Some(p) => SchemaChange::modify_column_at(column, p),
            None => SchemaChange::modify_column(column),
        },
    }
}

/// Parse a MySQL/TiDB `ALTER TABLE` statement into schema changes.
///
/// Covers the column operations the schema-evolution pipeline supports:
/// `ADD [COLUMN]`, `DROP [COLUMN]`, `MODIFY [COLUMN]`, `CHANGE [COLUMN]`,
/// `RENAME COLUMN ... TO ...`. Other clauses (index, comment, rename table)
/// are ignored and produce no changes. Returns `None` for non-ALTER
/// statements.
pub fn parse_alter_table(ddl: &str) -> Option<Vec<seatunnel_api::SchemaChange>> {
    use seatunnel_api::SchemaChange;

    let normalized = normalize_ws(ddl);
    let lower = normalized.to_lowercase();
    let rest = lower.strip_prefix("alter table ")?;
    // Skip the table name (possibly `db`.`tbl` or db.tbl).
    let after_table = skip_identifier(rest);
    let actions_src = &normalized[normalized.len() - after_table.len()..];

    let mut changes = Vec::new();
    for action in split_actions(actions_src) {
        let action_norm = normalize_ws(&action);
        let action_lower = action_norm.to_lowercase();
        // Non-column targets of ADD/DROP clauses.
        if action_lower.starts_with("add index ")
            || action_lower.starts_with("add key ")
            || action_lower.starts_with("add constraint ")
            || action_lower.starts_with("add primary ")
            || action_lower.starts_with("add unique ")
            || action_lower.starts_with("add foreign ")
            || action_lower.starts_with("add fulltext ")
            || action_lower.starts_with("add spatial ")
            || action_lower.starts_with("add check ")
            || action_lower.starts_with("drop index ")
            || action_lower.starts_with("drop key ")
            || action_lower.starts_with("drop primary ")
            || action_lower.starts_with("drop foreign ")
            || action_lower.starts_with("drop constraint ")
            || action_lower.starts_with("drop check ")
        {
            continue;
        }
        if action_lower.starts_with("add ") {
            let inner = strip_keyword(&action_norm, "add");
            let inner = strip_keyword(inner, "column");
            // Parenthesized multi-column list.
            if let Some(list) = inner.strip_prefix('(').and_then(|l| l.strip_suffix(')')) {
                for col in list.split(',') {
                    if let Some(def) = parse_column_def(col) {
                        changes.push(SchemaChange::add_column(def));
                    }
                }
            } else if let Some(def) = parse_column_def(inner) {
                changes.push(SchemaChange::add_column(def));
            }
        } else if action_lower.starts_with("drop ") {
            let inner = strip_keyword(&action_norm, "drop");
            let inner = strip_keyword(inner, "column");
            let name = inner
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('`');
            if !name.is_empty() {
                changes.push(SchemaChange::drop_column(name));
            }
        } else if action_lower.starts_with("modify ") {
            let inner = strip_keyword(&action_norm, "modify");
            let inner = strip_keyword(inner, "column");
            if let Some(def) = parse_column_def(inner) {
                changes.push(SchemaChange::modify_column(def));
            }
        } else if action_lower.starts_with("change ") {
            let inner = strip_keyword(&action_norm, "change");
            let inner = strip_keyword(inner, "column");
            let mut parts = inner.splitn(2, char::is_whitespace);
            let old = parts.next().unwrap_or("").trim_matches('`');
            if let Some(rest) = parts.next() {
                if let Some(def) = parse_column_def(rest) {
                    changes.push(SchemaChange::rename_column(old, def.name.clone()));
                    changes.push(SchemaChange::modify_column(def));
                }
            }
        } else if action_lower.starts_with("rename column ") {
            let inner = strip_keyword(&action_norm, "rename");
            let inner = strip_keyword(inner, "column");
            let to_pos = inner.to_lowercase().find(" to ")?;
            let old = inner[..to_pos].trim().trim_matches('`');
            let new = inner[to_pos + 4..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('`');
            if !old.is_empty() && !new.is_empty() {
                changes.push(SchemaChange::rename_column(old, new));
            }
        }
        // Other ALTER clauses (ADD INDEX, ALGORITHM=..., etc.) are ignored.
    }
    Some(changes)
}

/// Extract the target table name (last `db.tbl` component, backticks
/// stripped) from an `ALTER TABLE` statement; `None` for other statements.
pub fn alter_table_target(ddl: &str) -> Option<String> {
    let normalized = normalize_ws(ddl);
    let mut it = normalized.split_whitespace();
    if !it.next()?.eq_ignore_ascii_case("alter") {
        return None;
    }
    if !it.next()?.eq_ignore_ascii_case("table") {
        return None;
    }
    let ident = it.next()?;
    let name = ident.split('.').next_back().unwrap_or("").trim_matches('`');
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Skip `db`.`tbl` / db.tbl / tbl and return the remainder.
fn skip_identifier(s: &str) -> &str {
    let mut rest = s.trim_start();
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == '`');
        let word_end = rest
            .find(|c: char| c.is_whitespace() || c == '`' || c == '.')
            .unwrap_or(rest.len());
        rest = &rest[word_end..];
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == '`');
        if rest.starts_with('.') {
            rest = &rest[1..];
            continue;
        }
        return rest;
    }
}

fn strip_keyword<'a>(s: &'a str, keyword: &str) -> &'a str {
    if s.len() > keyword.len() + 1
        && s[..keyword.len()].eq_ignore_ascii_case(keyword)
        && s.as_bytes()[keyword.len()] == b' '
    {
        &s[keyword.len() + 1..]
    } else {
        s
    }
}

/// Split ALTER actions on top-level commas (parentheses aware).
fn split_actions(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(c);
            }
            ',' if depth == 0 => {
                out.push(current.clone());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

/// Parse "name type [NOT NULL|NULL] ..." into a ColumnDef.
fn parse_column_def(s: &str) -> Option<seatunnel_api::ColumnDef> {
    let s = s.trim().trim_matches('`');
    let mut tokens: Vec<&str> = s.split_whitespace().collect();
    let name = tokens.first()?.trim_matches('`').to_string();
    if name.is_empty() {
        return None;
    }
    tokens.remove(0);
    if tokens.is_empty() {
        return None;
    }
    // Type token possibly followed by (len[,scale]).
    let mut type_str = tokens.remove(0).to_lowercase();
    if tokens.first().map(|t| t.starts_with('(')).unwrap_or(false) {
        while !tokens.is_empty() {
            type_str.push_str(tokens.remove(0));
            if type_str.ends_with(')') {
                break;
            }
        }
    }
    let nullable = !tokens.join(" ").to_lowercase().contains("not null");
    let (base, len, scale) = parse_type_spec(&type_str);
    let dialect = seatunnel_api::schema::MySqlDialect;
    let column_type = seatunnel_api::schema::DatabaseDialect::map_type(&dialect, &base, len, scale);
    Some(
        seatunnel_api::ColumnDef::new(name, column_type)
            .nullable(nullable)
            .source_type(type_str),
    )
}

/// Split "decimal(10,2)" into ("decimal", Some(10), Some(2)).
pub fn parse_type_spec(ty: &str) -> (String, Option<u32>, Option<i8>) {
    if let Some(open) = ty.find('(') {
        let base = ty[..open].trim().to_string();
        let inner = ty[open + 1..].trim_end_matches(')');
        let mut nums = inner.split(',');
        let len = nums.next().and_then(|n| n.trim().parse::<u32>().ok());
        let scale = nums.next().and_then(|n| n.trim().parse::<i8>().ok());
        (base, len, scale)
    } else {
        (ty.trim().to_string(), None, None)
    }
}

/// Official SeaTunnel options that are accepted for configuration
/// compatibility but not implemented by this Rust engine; each present
/// option is logged once with an honest behavior note.
pub fn compatibility_warnings(config: &seatunnel_connector_common::ConnectorConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    const UNIMPLEMENTED: &[(&str, &str)] = &[
        ("exactly_once", "delivery stays at-least-once"),
        ("format", "positional row encoding is used"),
        ("debeziumConfig", "Debezium is not embedded in this engine"),
        ("debezium", "Debezium is not embedded in this engine"),
        (
            "chunk-key.even-distribution.factor.upper-bound",
            "fixed-range chunking is used",
        ),
        (
            "chunk-key.even-distribution.factor.lower-bound",
            "fixed-range chunking is used",
        ),
        ("sample-sharding.threshold", "sampling sharding is not used"),
        ("inverse-sampling.rate", "sampling sharding is not used"),
        ("int_type_narrowing", "binlog values decode positionally"),
        ("connect.timeout.ms", "handled by the driver pool defaults"),
        ("connect.max-retries", "handled by the reconnect loop"),
    ];
    for (key, note) in UNIMPLEMENTED {
        if config.get(key).is_some() {
            warnings.push(format!(
                "option '{}' accepted for compatibility but ignored ({})",
                key, note
            ));
        }
    }
    warnings
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
    pub fn new(
        hostname: &str,
        port: u16,
        username: &str,
        password: &str,
        database: &str,
        table: &str,
    ) -> Self {
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
    fn schema(&self) -> Option<&TableSchema> {
        None
    }
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
        !event_watermark.is_min() && event_watermark >= &self.low_watermark
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
        let split = IncrementalSplit::new("mydb", "users")
            .with_offset("file", "binlog.000001")
            .with_offset("pos", "12345");
        assert!(split.split_id().starts_with("incremental-"));
        assert_eq!(split.offset.get("file"), Some(&"binlog.000001".to_string()));
    }

    #[test]
    fn test_cdc_state() {
        let state = CdcState::new(CdcPhase::Incremental, HashMap::new())
            .with_watermark(Watermark::Value(42));
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
        assert!(buf.should_emit(&Watermark::Value(51))); // 51 >= 50, should emit
        assert!(!buf.should_emit(&Watermark::Value(49))); // 49 < 50, already emitted
        assert!(!buf.should_emit(&Watermark::Min));
    }

    #[test]
    fn test_diff_columns_add_drop_modify() {
        use seatunnel_api::{ColumnDef, ColumnType, SchemaChange};

        let old = vec![
            ColumnDef::new("id", ColumnType::Int64).primary_key(),
            ColumnDef::new("name", ColumnType::String),
        ];
        // add "email", drop "name", modify "id" -> Int32
        let new = vec![
            ColumnDef::new("id", ColumnType::Int32).primary_key(),
            ColumnDef::new("email", ColumnType::String),
        ];
        let events = diff_columns("db.t", &old, &new);
        assert_eq!(events.len(), 1);
        let changes = &events[0].changes;
        // email lands at ordinal 1 in the new layout (id, email)
        assert!(changes.contains(&SchemaChange::add_column_at(
            ColumnDef::new("email", ColumnType::String),
            1
        )));
        assert!(changes.contains(&SchemaChange::DropColumn {
            column_name: "name".to_string(),
            position: Some(1),
        }));
        assert!(changes.contains(&SchemaChange::modify_column_at(
            ColumnDef::new("id", ColumnType::Int32).primary_key(),
            0
        )));
    }

    #[test]
    fn test_diff_columns_rename_heuristic() {
        use seatunnel_api::{ColumnDef, ColumnType, SchemaChange};

        let old = vec![ColumnDef::new("id", ColumnType::Int64)];
        let new = vec![ColumnDef::new("uid", ColumnType::Int64)];
        let events = diff_columns("db.t", &old, &new);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].changes,
            vec![SchemaChange::rename_column("id", "uid")]
        );
    }

    #[test]
    fn test_diff_columns_no_change() {
        use seatunnel_api::{ColumnDef, ColumnType};

        let cols = vec![ColumnDef::new("id", ColumnType::Int64)];
        assert!(diff_columns("db.t", &cols, &cols).is_empty());
    }

    #[test]
    fn test_parse_alter_table_operations() {
        use seatunnel_api::{ColumnDef, ColumnType, SchemaChange};

        // ADD COLUMN
        let changes =
            parse_alter_table("ALTER TABLE `db`.`users` ADD COLUMN email VARCHAR(64) NOT NULL")
                .unwrap();
        assert_eq!(
            changes,
            vec![SchemaChange::add_column(
                ColumnDef::new("email", ColumnType::String)
                    .not_null()
                    .source_type("varchar(64)")
            )]
        );

        // DROP COLUMN
        let changes = parse_alter_table("ALTER TABLE users DROP COLUMN age").unwrap();
        assert_eq!(changes, vec![SchemaChange::drop_column("age")]);

        // MODIFY COLUMN with type change
        let changes = parse_alter_table("ALTER TABLE users MODIFY score DECIMAL(10,2)").unwrap();
        assert_eq!(
            changes,
            vec![SchemaChange::modify_column(
                ColumnDef::new(
                    "score",
                    ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                )
                .source_type("decimal(10,2)")
            )]
        );

        // CHANGE COLUMN = rename + modify
        let changes =
            parse_alter_table("ALTER TABLE users CHANGE COLUMN name full_name VARCHAR(128)")
                .unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0], SchemaChange::rename_column("name", "full_name"));

        // RENAME COLUMN
        let changes = parse_alter_table("ALTER TABLE users RENAME COLUMN a TO b").unwrap();
        assert_eq!(changes, vec![SchemaChange::rename_column("a", "b")]);

        // Multi-action statement
        let changes = parse_alter_table("ALTER TABLE t ADD c1 INT, DROP c2").unwrap();
        assert_eq!(changes.len(), 2);

        // Ignored clause types produce no changes; non-ALTER is None.
        assert!(
            parse_alter_table("ALTER TABLE t ADD INDEX idx_name (name)")
                .unwrap()
                .is_empty()
        );
        assert!(parse_alter_table("CREATE TABLE t (a INT)").is_none());
        assert!(parse_alter_table("BEGIN").is_none());
    }

    #[test]
    fn test_schema_filters_include_exclude() {
        let config = SchemaEvolutionConfig {
            enabled: true,
            include: vec!["email".to_string()],
            exclude: vec![],
            ..SchemaEvolutionConfig::default()
        };
        let mut watcher = SchemaWatcher::new("db.t", &config);
        watcher.observe_ddl("ALTER TABLE t ADD COLUMN email VARCHAR(8)");
        watcher.observe_ddl("ALTER TABLE t ADD COLUMN phone VARCHAR(8)");
        assert!(watcher.take_pending().is_some()); // email passes
        assert!(watcher.take_pending().is_none()); // phone filtered out

        let config = SchemaEvolutionConfig {
            enabled: true,
            include: vec![],
            exclude: vec!["email".to_string()],
            ..SchemaEvolutionConfig::default()
        };
        let mut watcher = SchemaWatcher::new("db.t", &config);
        watcher.observe_ddl("ALTER TABLE t ADD COLUMN email VARCHAR(8)");
        assert!(watcher.take_pending().is_none()); // excluded
    }

    #[test]
    fn test_alter_table_target() {
        assert_eq!(
            alter_table_target("ALTER TABLE `db`.`users` ADD COLUMN email VARCHAR(64)"),
            Some("users".to_string())
        );
        assert_eq!(
            alter_table_target("alter table users drop column x"),
            Some("users".to_string())
        );
        assert_eq!(alter_table_target("CREATE TABLE t (a INT)"), None);
    }

    #[test]
    fn test_watcher_observe_ddl_queues_event() {
        let config = SchemaEvolutionConfig {
            enabled: true,
            poll_interval_ms: 1000,
            include: Vec::new(),
            exclude: Vec::new(),
        };
        let mut watcher = SchemaWatcher::new("db.users", &config);
        watcher.prime(vec![
            seatunnel_api::ColumnDef::new("id", seatunnel_api::ColumnType::Int64).primary_key(),
        ]);
        watcher.observe_ddl("ALTER TABLE users ADD COLUMN email VARCHAR(64)");
        let event = watcher.take_pending().expect("event queued");
        assert_eq!(event.changes.len(), 1);
        assert!(watcher.take_pending().is_none());
        // Baseline updated.
        assert!(watcher.columns().iter().any(|c| c.name == "email"));
    }
}
