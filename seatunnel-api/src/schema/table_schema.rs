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

use super::ColumnDef;

/// A table schema discovered from a database.
///
/// Mirrors `org.apache.seatunnel.api.table.catalog.TableSchema` from the Java version.
/// Contains the complete type-safe schema for a single table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Fully qualified table name (e.g., "database.table" or "schema.table")
    pub table_identifier: String,
    /// Column definitions in order
    pub columns: Vec<ColumnDef>,
    /// Primary key column names
    pub primary_key: Vec<String>,
    /// Table comment
    pub comment: Option<String>,
}

impl TableSchema {
    /// Create a new table schema.
    pub fn new(table_identifier: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        let primary_key: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key())
            .map(|c| c.name.clone())
            .collect();

        TableSchema {
            table_identifier: table_identifier.into(),
            columns,
            primary_key,
            comment: None,
        }
    }

    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get the index of a column by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if the table has a primary key.
    pub fn has_primary_key(&self) -> bool {
        !self.primary_key.is_empty()
    }

    /// Get the primary key columns.
    pub fn primary_key_columns(&self) -> &[String] {
        &self.primary_key
    }

    /// Get field names in order.
    pub fn field_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get column types in order.
    pub fn column_types(&self) -> Vec<&crate::row::ColumnType> {
        self.columns.iter().map(|c| &c.column_type).collect()
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TableSchema('{}')", self.table_identifier)?;
        writeln!(f, "  Columns:")?;
        for col in &self.columns {
            writeln!(f, "    - {}", col)?;
        }
        if !self.primary_key.is_empty() {
            writeln!(f, "  Primary Key: [{}]", self.primary_key.join(", "))?;
        }
        Ok(())
    }
}
