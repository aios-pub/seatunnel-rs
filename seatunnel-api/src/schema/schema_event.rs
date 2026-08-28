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

//! Schema change events for streaming schema evolution.
//!
//! Mirrors `org.apache.seatunnel.api.table.schema.event.*` from the Java
//! version. A CDC source detects DDL on the captured table (add / drop /
//! rename column, column type change) and emits a [`SchemaChangeEvent`]
//! through the data stream; the engine forwards it to the sink, which
//! applies the change to its own storage (e.g. `ALTER TABLE` for JDBC,
//! mapping update for Elasticsearch) before any row with the new shape
//! is written.

use super::{ColumnDef, TableSchema};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors produced while applying a schema change to a [`TableSchema`].
#[derive(Debug, Error, PartialEq)]
pub enum SchemaChangeError {
    #[error("column '{0}' already exists")]
    ColumnExists(String),
    #[error("column '{0}' does not exist")]
    ColumnNotFound(String),
    #[error("cannot drop column '{0}': it is part of the primary key")]
    CannotDropPrimaryKey(String),
}

/// A single atomic schema modification on one column.
///
/// `position` (0-based ordinal in the new table layout) lets positional
/// sinks — which know columns only as `f0..fN` — translate source column
/// names into their own naming scheme.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaChange {
    /// `ALTER TABLE ... ADD COLUMN`
    AddColumn {
        column: ColumnDef,
        #[serde(default)]
        position: Option<usize>,
    },
    /// `ALTER TABLE ... DROP COLUMN`
    DropColumn {
        column_name: String,
        #[serde(default)]
        position: Option<usize>,
    },
    /// `ALTER TABLE ... RENAME COLUMN old TO new`
    RenameColumn {
        old_name: String,
        new_name: String,
        #[serde(default)]
        position: Option<usize>,
    },
    /// Column type (and/or nullability) change: `ALTER TABLE ... MODIFY/ALTER COLUMN`.
    /// `column.name` identifies the column; the definition carries the new type.
    ModifyColumn {
        column: ColumnDef,
        #[serde(default)]
        position: Option<usize>,
    },
}

impl SchemaChange {
    pub fn add_column(column: ColumnDef) -> Self {
        SchemaChange::AddColumn {
            column,
            position: None,
        }
    }

    pub fn add_column_at(column: ColumnDef, position: usize) -> Self {
        SchemaChange::AddColumn {
            column,
            position: Some(position),
        }
    }

    pub fn drop_column(column_name: impl Into<String>) -> Self {
        SchemaChange::DropColumn {
            column_name: column_name.into(),
            position: None,
        }
    }

    pub fn rename_column(old_name: impl Into<String>, new_name: impl Into<String>) -> Self {
        SchemaChange::RenameColumn {
            old_name: old_name.into(),
            new_name: new_name.into(),
            position: None,
        }
    }

    pub fn modify_column(column: ColumnDef) -> Self {
        SchemaChange::ModifyColumn {
            column,
            position: None,
        }
    }

    pub fn modify_column_at(column: ColumnDef, position: usize) -> Self {
        SchemaChange::ModifyColumn {
            column,
            position: Some(position),
        }
    }

    /// The source-side column name this change operates on.
    pub fn column_name(&self) -> &str {
        match self {
            SchemaChange::AddColumn { column, .. } | SchemaChange::ModifyColumn { column, .. } => {
                &column.name
            }
            SchemaChange::DropColumn { column_name, .. } => column_name,
            SchemaChange::RenameColumn { old_name, .. } => old_name,
        }
    }
}

/// Translate a change onto a positional sink schema (`f0..fN` column
/// names). No-op when the change carries no position or already matches.
pub fn translate_positional(change: &SchemaChange) -> SchemaChange {
    match change {
        SchemaChange::AddColumn { column, position } => match position {
            Some(pos) => SchemaChange::add_column_at(
                ColumnDef::new(format!("f{}", pos), column.column_type.clone())
                    .nullable(column.nullable)
                    .with_primary_key(column.primary_key)
                    .source_type(column.source_type.clone().unwrap_or_default()),
                *pos,
            ),
            None => change.clone(),
        },
        SchemaChange::DropColumn {
            column_name: _,
            position,
        } => match position {
            Some(pos) => SchemaChange::DropColumn {
                column_name: format!("f{}", pos),
                position: Some(*pos),
            },
            None => change.clone(),
        },
        SchemaChange::RenameColumn {
            old_name: _,
            new_name: _,
            position,
        } => match position {
            Some(pos) => SchemaChange::RenameColumn {
                old_name: format!("f{}", pos),
                new_name: format!("f{}", pos),
                position: Some(*pos),
            },
            None => change.clone(),
        },
        SchemaChange::ModifyColumn { column, position } => match position {
            Some(pos) => SchemaChange::modify_column_at(
                ColumnDef::new(format!("f{}", pos), column.column_type.clone())
                    .nullable(column.nullable)
                    .with_primary_key(column.primary_key)
                    .source_type(column.source_type.clone().unwrap_or_default()),
                *pos,
            ),
            None => change.clone(),
        },
    }
}

/// A schema change on a single table, carried through the data stream.
///
/// The Java counterpart batches per-ALTER-statement events into
/// `AlterTableColumnsEvent`; here a batch of changes is simply a `Vec`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaChangeEvent {
    /// Table identifier as seen by the sink (e.g. "database.table").
    pub table: String,
    /// Column changes in statement order.
    pub changes: Vec<SchemaChange>,
    /// Raw DDL statement (if the change originated from a captured DDL).
    pub statement: Option<String>,
    /// Full schema snapshot for initial-schema events: sources emit one
    /// per captured table BEFORE its first row so schema-driven sinks
    /// (e.g. the canal-client format) can configure themselves without
    /// static column config. `changes` is empty for these events.
    #[serde(default)]
    pub initial_schema: Option<TableSchema>,
}

impl SchemaChangeEvent {
    pub fn new(table: impl Into<String>, changes: Vec<SchemaChange>) -> Self {
        SchemaChangeEvent {
            table: table.into(),
            changes,
            statement: None,
            initial_schema: None,
        }
    }

    /// An initial-schema event: the table's full column layout, no changes.
    pub fn initial_schema(schema: TableSchema) -> Self {
        SchemaChangeEvent {
            table: schema.table_identifier.clone(),
            changes: Vec::new(),
            statement: None,
            initial_schema: Some(schema),
        }
    }

    pub fn with_statement(mut self, statement: impl Into<String>) -> Self {
        self.statement = Some(statement.into());
        self
    }

    /// The schema snapshot carried by an initial-schema event (None for
    /// regular DDL change events).
    pub fn initial_schema_snapshot(&self) -> Option<&TableSchema> {
        self.initial_schema.as_ref()
    }
}

impl TableSchema {
    /// Apply one schema change to this table schema.
    pub fn apply_schema_change(&mut self, change: &SchemaChange) -> Result<(), SchemaChangeError> {
        match change {
            SchemaChange::AddColumn { column, .. } => {
                if self.column_index(&column.name).is_some() {
                    return Err(SchemaChangeError::ColumnExists(column.name.clone()));
                }
                let pk = column.primary_key;
                if pk {
                    self.primary_key.push(column.name.clone());
                }
                self.columns.push(column.clone());
                Ok(())
            }
            SchemaChange::DropColumn { column_name, .. } => {
                if self.primary_key.iter().any(|pk| pk == column_name) {
                    return Err(SchemaChangeError::CannotDropPrimaryKey(column_name.clone()));
                }
                let idx = self
                    .column_index(column_name)
                    .ok_or_else(|| SchemaChangeError::ColumnNotFound(column_name.clone()))?;
                self.columns.remove(idx);
                Ok(())
            }
            SchemaChange::RenameColumn {
                old_name, new_name, ..
            } => {
                let idx = self
                    .column_index(old_name)
                    .ok_or_else(|| SchemaChangeError::ColumnNotFound(old_name.clone()))?;
                self.columns[idx].name = new_name.clone();
                if let Some(pk) = self
                    .primary_key
                    .iter_mut()
                    .find(|pk| pk.as_str() == old_name)
                {
                    *pk = new_name.clone();
                }
                Ok(())
            }
            SchemaChange::ModifyColumn { column, .. } => {
                let idx = self
                    .column_index(&column.name)
                    .ok_or_else(|| SchemaChangeError::ColumnNotFound(column.name.clone()))?;
                self.columns[idx].column_type = column.column_type.clone();
                self.columns[idx].nullable = column.nullable;
                if let Some(source_type) = &column.source_type {
                    self.columns[idx].source_type = Some(source_type.clone());
                }
                Ok(())
            }
        }
    }

    /// Apply all changes of an event in order; stops at the first error.
    pub fn apply_schema_change_event(
        &mut self,
        event: &SchemaChangeEvent,
    ) -> Result<(), SchemaChangeError> {
        for change in &event.changes {
            self.apply_schema_change(change)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::ColumnType;

    fn schema() -> TableSchema {
        TableSchema::new(
            "db.t",
            vec![
                ColumnDef::new("id", ColumnType::Int64).primary_key(),
                ColumnDef::new("name", ColumnType::String),
            ],
        )
    }

    #[test]
    fn test_add_drop_modify_rename() {
        let mut s = schema();

        s.apply_schema_change(&SchemaChange::add_column(ColumnDef::new(
            "score",
            ColumnType::Int32,
        )))
        .unwrap();
        assert_eq!(s.column_count(), 3);

        s.apply_schema_change(&SchemaChange::rename_column("name", "full_name"))
            .unwrap();
        assert!(s.column_index("full_name").is_some());
        assert!(s.column_index("name").is_none());

        s.apply_schema_change(&SchemaChange::modify_column(ColumnDef::new(
            "score",
            ColumnType::Float64,
        )))
        .unwrap();
        assert_eq!(
            s.get_column("score").unwrap().column_type,
            ColumnType::Float64
        );

        s.apply_schema_change(&SchemaChange::drop_column("full_name"))
            .unwrap();
        assert_eq!(s.column_count(), 2);

        // Dropping a primary key column is rejected.
        assert_eq!(
            s.apply_schema_change(&SchemaChange::drop_column("id")),
            Err(SchemaChangeError::CannotDropPrimaryKey("id".to_string()))
        );
        // Unknown column errors.
        assert_eq!(
            s.apply_schema_change(&SchemaChange::drop_column("ghost")),
            Err(SchemaChangeError::ColumnNotFound("ghost".to_string()))
        );
    }

    #[test]
    fn test_event_roundtrip_serde() {
        let event = SchemaChangeEvent::new(
            "db.t",
            vec![
                SchemaChange::add_column(ColumnDef::new("c", ColumnType::String).nullable(true)),
                SchemaChange::modify_column(ColumnDef::new(
                    "n",
                    ColumnType::Decimal {
                        precision: 20,
                        scale: 4,
                    },
                )),
            ],
        )
        .with_statement("ALTER TABLE t ADD COLUMN c varchar(64)");
        let json = serde_json::to_string(&event).unwrap();
        let back: SchemaChangeEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, event);
    }
}
