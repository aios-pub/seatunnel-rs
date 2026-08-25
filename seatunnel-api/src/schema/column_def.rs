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

use crate::row::ColumnType;

/// A column definition in a table schema.
///
/// Mirrors `org.apache.seatunnel.api.table.catalog.Column` from the Java version,
/// but with compile-time type safety via the `ColumnType` enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column SQL type (precisely mapped from database metadata)
    pub column_type: ColumnType,
    /// Whether the column can be NULL
    pub nullable: bool,
    /// Whether this column is part of the primary key
    pub primary_key: bool,
    /// Default value (if any)
    pub default_value: Option<String>,
    /// Column comment
    pub comment: Option<String>,
    /// Original database type name (e.g., "varchar(50)", "tinyint(1)")
    pub source_type: Option<String>,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        ColumnDef {
            name: name.into(),
            column_type,
            nullable: true,
            primary_key: false,
            default_value: None,
            comment: None,
            source_type: None,
        }
    }

    /// Mark this column as part of the primary key.
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self
    }

    /// Allow this column to be NULL.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Mark this column as NOT NULL.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set a default value.
    pub fn default_value(mut self, value: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self
    }

    /// Set a comment.
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Set the original source type string.
    pub fn source_type(mut self, source_type: impl Into<String>) -> Self {
        self.source_type = Some(source_type.into());
        self
    }

    /// Get the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column type.
    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    /// Check if this is a primary key column.
    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    /// Check if this column allows NULL.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

impl std::fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.column_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        if let Some(default) = &self.default_value {
            write!(f, " DEFAULT '{default}'")?;
        }
        Ok(())
    }
}
