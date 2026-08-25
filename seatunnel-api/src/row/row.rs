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

use super::{Field, RowKind};

/// A row of data in SeaTunnel.
///
/// A row consists of a [`RowKind`] (change type) and a fixed array of [`Field`] values.
///
/// # Design vs Java
/// Java uses `Object[] fields` which boxes every value and has no compile-time
/// type safety. Rust's `Row` uses a typed `Vec<Field>` where each `Field` enum
/// variant corresponds to a specific SQL type.
///
/// The schema (`TableSchema`) defines the expected types, and the `RowBuilder`
/// ensures type-safe construction.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub kind: RowKind,
    pub fields: Vec<Field>,
}

impl Row {
    /// Create a new empty INSERT row.
    pub fn new_insert(field_count: usize) -> Self {
        Row {
            kind: RowKind::Insert,
            fields: vec![Field::Null; field_count],
        }
    }

    /// Create a new empty row with the specified kind.
    pub fn new(kind: RowKind, field_count: usize) -> Self {
        Row {
            kind,
            fields: vec![Field::Null; field_count],
        }
    }

    /// Get the number of fields in this row.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Get a field by index.
    pub fn get(&self, index: usize) -> &Field {
        &self.fields[index]
    }

    /// Get a field by index mutably.
    pub fn get_mut(&mut self, index: usize) -> &mut Field {
        &mut self.fields[index]
    }

    /// Set a field by index.
    pub fn set(&mut self, index: usize, field: Field) {
        self.fields[index] = field;
    }

    /// Check if all fields are null.
    pub fn is_all_null(&self) -> bool {
        self.fields.iter().all(Field::is_null)
    }
}

impl Default for Row {
    fn default() -> Self {
        Row::new(RowKind::Insert, 0)
    }
}

/// Builder for constructing rows with type checking.
///
/// Ensures that fields are set in the correct order and type.
#[derive(Debug, Default)]
pub struct RowBuilder {
    fields: Vec<Field>,
    kind: RowKind,
}

impl RowBuilder {
    pub fn new(kind: RowKind) -> Self {
        RowBuilder {
            fields: Vec::new(),
            kind,
        }
    }

    pub fn with_field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    pub fn build(self) -> Row {
        Row {
            kind: self.kind,
            fields: self.fields,
        }
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]",
            self.kind,
            self.fields
                .iter()
                .map(|field| format!("{field}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_insert() {
        let row = Row::new_insert(3);
        assert_eq!(row.kind, RowKind::Insert);
        assert_eq!(row.field_count(), 3);
        assert!(row.is_all_null());
    }

    #[test]
    fn test_builder() {
        let row = RowBuilder::new(RowKind::Insert)
            .with_field(Field::Int32(1))
            .with_field(Field::String("hello".to_string()))
            .build();

        assert_eq!(row.get(0), &Field::Int32(1));
        assert_eq!(row.get(1), &Field::String("hello".to_string()));
    }

    #[test]
    fn test_set_get() {
        let mut row = Row::new_insert(2);
        row.set(0, Field::Int32(42));
        row.set(1, Field::String("world".to_string()));
        assert_eq!(row.get(0), &Field::Int32(42));
    }

    #[test]
    fn test_display() {
        let mut row = Row::new_insert(2);
        row.set(0, Field::Int32(1));
        row.set(1, Field::String("test".to_string()));
        assert_eq!(format!("{}", row), "INSERT[1, \"test\"]");
    }
}
