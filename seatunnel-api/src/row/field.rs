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

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};

use super::ColumnType;

/// A single field value in a row.
///
/// This is the Rust equivalent of Java's `Object` field in `SeaTunnelRow`.
/// Unlike Java's boxed `Object[]`, this enum provides compile-time type safety
/// while still supporting arbitrary schemas discovered from databases.
///
/// # Design vs Java
/// Java's `SeaTunnelRow` uses `Object[] fields` — every value is a boxed object.
/// This leads to runtime type errors and cross-table type mismatches.
///
/// Rust's `Field` enum matches the schema's `ColumnType` at the type level,
/// preventing mismatches at compile time when used with proper typing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Field {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal(BigDecimal),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Duration(i64), // nanoseconds
    Array(Vec<Field>),
    Row(Vec<Field>),
}

impl Field {
    /// Create a NULL field.
    pub const fn null() -> Self {
        Field::Null
    }

    /// Get the column type that this field value corresponds to.
    /// Returns `None` for Null fields.
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Field::Null => None,
            Field::Bool(_) => Some(ColumnType::Bool),
            Field::Int8(_) => Some(ColumnType::Int8),
            Field::Int16(_) => Some(ColumnType::Int16),
            Field::Int32(_) => Some(ColumnType::Int32),
            Field::Int64(_) => Some(ColumnType::Int64),
            Field::UInt8(_) => Some(ColumnType::UInt8),
            Field::UInt16(_) => Some(ColumnType::UInt16),
            Field::UInt32(_) => Some(ColumnType::UInt32),
            Field::UInt64(_) => Some(ColumnType::UInt64),
            Field::Float32(_) => Some(ColumnType::Float32),
            Field::Float64(_) => Some(ColumnType::Float64),
            Field::Decimal(_) => Some(ColumnType::Decimal {
                precision: 38,
                scale: 0,
            }),
            Field::String(_) => Some(ColumnType::String),
            Field::Bytes(_) => Some(ColumnType::Bytes),
            Field::Json(_) => Some(ColumnType::Json),
            Field::Date(_) => Some(ColumnType::Date),
            Field::Time(_) => Some(ColumnType::Time),
            Field::DateTime(_) => Some(ColumnType::DateTime),
            Field::TimestampTz(_) => Some(ColumnType::TimestampTz),
            Field::Duration(_) => Some(ColumnType::Duration),
            Field::Array(_) => Some(ColumnType::Array {
                element_type: Box::new(ColumnType::String),
            }),
            Field::Row(_) => Some(ColumnType::String),
        }
    }

    /// Try to downcast to a specific type. Returns `None` if the type doesn't match.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Field::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Field::Int8(v) => Some(*v as i32),
            Field::Int16(v) => Some(*v as i32),
            Field::Int32(v) => Some(*v),
            Field::Int64(v) => {
                let r = i32::try_from(*v).ok()?;
                Some(r)
            }
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Field::Int8(v) => Some(*v as i64),
            Field::Int16(v) => Some(*v as i64),
            Field::Int32(v) => Some(*v as i64),
            Field::Int64(v) => Some(*v),
            Field::UInt8(v) => Some(*v as i64),
            Field::UInt16(v) => Some(*v as i64),
            Field::UInt32(v) => Some(*v as i64),
            Field::UInt64(v) => {
                let r = i64::try_from(*v).ok()?;
                Some(r)
            }
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Field::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Field::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Field::Null)
    }
}

impl From<bool> for Field {
    fn from(v: bool) -> Self {
        Field::Bool(v)
    }
}

impl From<i8> for Field {
    fn from(v: i8) -> Self {
        Field::Int8(v)
    }
}

impl From<i16> for Field {
    fn from(v: i16) -> Self {
        Field::Int16(v)
    }
}

impl From<i32> for Field {
    fn from(v: i32) -> Self {
        Field::Int32(v)
    }
}

impl From<i64> for Field {
    fn from(v: i64) -> Self {
        Field::Int64(v)
    }
}

impl From<u8> for Field {
    fn from(v: u8) -> Self {
        Field::UInt8(v)
    }
}

impl From<u16> for Field {
    fn from(v: u16) -> Self {
        Field::UInt16(v)
    }
}

impl From<u32> for Field {
    fn from(v: u32) -> Self {
        Field::UInt32(v)
    }
}

impl From<u64> for Field {
    fn from(v: u64) -> Self {
        Field::UInt64(v)
    }
}

impl From<f32> for Field {
    fn from(v: f32) -> Self {
        Field::Float32(v)
    }
}

impl From<f64> for Field {
    fn from(v: f64) -> Self {
        Field::Float64(v)
    }
}

impl From<String> for Field {
    fn from(v: String) -> Self {
        Field::String(v)
    }
}

impl From<&str> for Field {
    fn from(v: &str) -> Self {
        Field::String(v.to_string())
    }
}

impl From<Vec<u8>> for Field {
    fn from(v: Vec<u8>) -> Self {
        Field::Bytes(v)
    }
}

impl From<chrono::NaiveDate> for Field {
    fn from(v: chrono::NaiveDate) -> Self {
        Field::Date(v)
    }
}

impl From<chrono::NaiveDateTime> for Field {
    fn from(v: chrono::NaiveDateTime) -> Self {
        Field::DateTime(v)
    }
}

impl From<DateTime<Utc>> for Field {
    fn from(v: DateTime<Utc>) -> Self {
        Field::TimestampTz(v)
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Field::Null => write!(f, "NULL"),
            Field::Bool(v) => write!(f, "{v}"),
            Field::Int8(v) => write!(f, "{v}"),
            Field::Int16(v) => write!(f, "{v}"),
            Field::Int32(v) => write!(f, "{v}"),
            Field::Int64(v) => write!(f, "{v}"),
            Field::UInt8(v) => write!(f, "{v}"),
            Field::UInt16(v) => write!(f, "{v}"),
            Field::UInt32(v) => write!(f, "{v}"),
            Field::UInt64(v) => write!(f, "{v}"),
            Field::Float32(v) => write!(f, "{v}"),
            Field::Float64(v) => write!(f, "{v}"),
            Field::Decimal(v) => write!(f, "{v}"),
            Field::String(v) => write!(f, "\"{v}\""),
            Field::Bytes(v) => write!(f, "0x{}", hex::encode(v)),
            Field::Json(v) => write!(f, "{v}"),
            Field::Date(v) => write!(f, "{v}"),
            Field::Time(v) => write!(f, "{v}"),
            Field::DateTime(v) => write!(f, "{v}"),
            Field::TimestampTz(v) => write!(f, "{v}"),
            Field::Duration(v) => write!(f, "{v}ns"),
            Field::Array(v) => write!(
                f,
                "[{}]",
                v.iter()
                    .map(|f| format!("{f}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Field::Row(v) => write!(
                f,
                "({})",
                v.iter()
                    .map(|f| format!("{f}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_traits() {
        let f: Field = true.into();
        assert_eq!(f, Field::Bool(true));

        let f: Field = 42i32.into();
        assert_eq!(f, Field::Int32(42));

        let f: Field = "hello".into();
        assert_eq!(f, Field::String("hello".to_string()));
    }

    #[test]
    fn test_as_methods() {
        let f = Field::Int32(42);
        assert_eq!(f.as_i32(), Some(42));
        assert_eq!(f.as_i64(), Some(42));
        assert_eq!(f.as_str(), None);

        let f = Field::String("hello".to_string());
        assert_eq!(f.as_str(), Some("hello"));
        assert_eq!(f.as_i32(), None);
    }

    #[test]
    fn test_is_null() {
        assert!(Field::Null.is_null());
        assert!(!Field::Int32(0).is_null());
        assert!(!Field::String(String::new()).is_null());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Field::Null), "NULL");
        assert_eq!(format!("{}", Field::Bool(true)), "true");
        assert_eq!(
            format!("{}", Field::String("hello".to_string())),
            "\"hello\""
        );
    }
}
