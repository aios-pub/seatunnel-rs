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

use std::fmt;

/// SQL column type system.
///
/// Mirrors the type system from [sea-orm](https://github.com/SeaQL/sea-orm)
/// but extended for CDC and streaming data scenarios.
///
/// # Type Safety
/// Unlike the Java version which infers types at runtime (leading to
/// cross-table type mismatches), this enum provides compile-time type
/// safety. Each connector discovers the exact type from the database
/// schema and maps it precisely.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnType {
    // Boolean
    Bool,

    // Signed integers
    Int8,
    Int16,
    Int32,
    Int64,

    // Unsigned integers
    UInt8,
    UInt16,
    UInt32,
    UInt64,

    // Floating point
    Float32,
    Float64,

    // Decimal (arbitrary precision)
    Decimal {
        precision: u8,
        scale: i8,
    },

    // String types
    String,

    // Binary types
    Bytes,

    // JSON
    Json,

    // Date/time types
    Date,
    Time,
    DateTime,    // YYYY-MM-DD HH:MM:SS (no timezone)
    TimestampTz, // With timezone (TIMESTAMP WITH TIME ZONE)
    Duration,

    // Composite types
    Array {
        element_type: Box<ColumnType>,
    },
    Map {
        key_type: Box<ColumnType>,
        value_type: Box<ColumnType>,
    },

    // Nullable wrapper
    Nullable(Box<ColumnType>),
}

impl ColumnType {
    /// Returns true if this type can hold NULL values.
    /// All types are nullable in SeaTunnel — this indicates whether
    /// the underlying database type is typically nullable.
    pub fn is_nullable(&self) -> bool {
        matches!(self, ColumnType::Nullable(_))
    }

    /// Returns the nullability wrapper if present.
    pub fn as_nullable(&self) -> Option<&ColumnType> {
        match self {
            ColumnType::Nullable(inner) => Some(inner.as_ref()),
            _ => None,
        }
    }

    /// Unwrap nullable wrapper, return the inner type.
    pub fn unnested(&self) -> &ColumnType {
        match self {
            ColumnType::Nullable(inner) => inner.unnested(),
            _ => self,
        }
    }

    /// Check if this is a numeric type.
    pub fn is_numeric(&self) -> bool {
        let t = self.unnested();
        matches!(
            t,
            ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::Int64
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Float32
                | ColumnType::Float64
                | ColumnType::Decimal { .. }
        )
    }

    /// Check if this is a temporal type.
    pub fn is_temporal(&self) -> bool {
        let t = self.unnested();
        matches!(
            t,
            ColumnType::Date | ColumnType::Time | ColumnType::TimestampTz | ColumnType::Duration
        )
    }

    /// Check if this is a string-like type.
    pub fn is_string(&self) -> bool {
        matches!(self.unnested(), ColumnType::String | ColumnType::Json)
    }

    /// Get a human-readable name for this type.
    pub fn type_name(&self) -> &'static str {
        match self.unnested() {
            ColumnType::Bool => "BOOLEAN",
            ColumnType::Int8 => "TINYINT",
            ColumnType::Int16 => "SMALLINT",
            ColumnType::Int32 => "INT",
            ColumnType::Int64 => "BIGINT",
            ColumnType::UInt8 => "TINYINT UNSIGNED",
            ColumnType::UInt16 => "SMALLINT UNSIGNED",
            ColumnType::UInt32 => "INT UNSIGNED",
            ColumnType::UInt64 => "BIGINT UNSIGNED",
            ColumnType::Float32 => "FLOAT",
            ColumnType::Float64 => "DOUBLE",
            ColumnType::Decimal { precision, scale } => {
                if *scale == 0 {
                    Box::leak(format!("DECIMAL({precision})").into_boxed_str())
                } else {
                    Box::leak(format!("DECIMAL({precision},{scale})").into_boxed_str())
                }
            }
            ColumnType::String => "STRING",
            ColumnType::Bytes => "BYTES",
            ColumnType::Json => "JSON",
            ColumnType::Date => "DATE",
            ColumnType::Time => "TIME",
            ColumnType::DateTime => "DATETIME",
            ColumnType::TimestampTz => "TIMESTAMP_TZ",
            ColumnType::Duration => "INTERVAL",
            ColumnType::Array { .. } => "ARRAY",
            ColumnType::Map { .. } => "MAP",
            ColumnType::Nullable(inner) => inner.type_name(),
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.type_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_name() {
        assert_eq!(ColumnType::Bool.type_name(), "BOOLEAN");
        assert_eq!(ColumnType::Int32.type_name(), "INT");
        assert_eq!(ColumnType::Int64.type_name(), "BIGINT");
        assert_eq!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .type_name(),
            "DECIMAL(10,2)"
        );
        assert_eq!(ColumnType::String.type_name(), "STRING");
        assert_eq!(ColumnType::TimestampTz.type_name(), "TIMESTAMP_TZ");
    }

    #[test]
    fn test_nullable() {
        let nullable_int = ColumnType::Nullable(Box::new(ColumnType::Int32));
        assert!(nullable_int.is_nullable());
        assert_eq!(nullable_int.unnested(), &ColumnType::Int32);
        assert_eq!(nullable_int.type_name(), "INT");
    }

    #[test]
    fn test_is_numeric() {
        assert!(ColumnType::Int32.is_numeric());
        assert!(ColumnType::Decimal {
            precision: 10,
            scale: 2
        }
        .is_numeric());
        assert!(!ColumnType::String.is_numeric());
        assert!(!ColumnType::Bool.is_numeric());
    }

    #[test]
    fn test_is_temporal() {
        assert!(ColumnType::Date.is_temporal());
        assert!(ColumnType::TimestampTz.is_temporal());
        assert!(!ColumnType::DateTime.is_temporal()); // DateTime is not temporal in our system
        assert!(!ColumnType::String.is_temporal());
    }
}
