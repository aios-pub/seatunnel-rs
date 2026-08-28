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

//! Driver-neutral SQL value + Field conversions for both the MySQL wire
//! protocol (mysql_async) and Postgres (tokio-postgres).

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use seatunnel_api::{ColumnType, Field};

/// A driver-neutral SQL value.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
}

impl SqlValue {
    /// Render as a text parameter (Postgres binds most values as text and
    /// casts at the placeholder).
    pub fn as_text(&self) -> Option<String> {
        match self {
            SqlValue::Null => None,
            SqlValue::Bool(b) => Some(b.to_string()),
            SqlValue::Int(v) => Some(v.to_string()),
            SqlValue::UInt(v) => Some(v.to_string()),
            SqlValue::Float(v) => Some(v.to_string()),
            SqlValue::Str(s) => Some(s.clone()),
            SqlValue::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
            SqlValue::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            SqlValue::Time(t) => Some(t.format("%H:%M:%S%.f").to_string()),
            SqlValue::DateTime(dt) => Some(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
            SqlValue::TimestampTz(ts) => Some(ts.to_rfc3339()),
        }
    }
}

// ---------------------------------------------------------------------------
// Field → SqlValue
// ---------------------------------------------------------------------------

pub fn field_to_sql_value(field: &Field) -> SqlValue {
    match field {
        Field::Null => SqlValue::Null,
        Field::Bool(b) => SqlValue::Bool(*b),
        Field::Int8(v) => SqlValue::Int(*v as i64),
        Field::Int16(v) => SqlValue::Int(*v as i64),
        Field::Int32(v) => SqlValue::Int(*v as i64),
        Field::Int64(v) => SqlValue::Int(*v),
        Field::UInt8(v) => SqlValue::UInt(*v as u64),
        Field::UInt16(v) => SqlValue::UInt(*v as u64),
        Field::UInt32(v) => SqlValue::UInt(*v as u64),
        Field::UInt64(v) => SqlValue::UInt(*v),
        Field::Float32(v) => SqlValue::Float(*v as f64),
        Field::Float64(v) => SqlValue::Float(*v),
        Field::Decimal(d) => SqlValue::Str(d.to_string()),
        Field::String(s) => SqlValue::Str(s.clone()),
        Field::Bytes(b) => SqlValue::Bytes(b.clone()),
        Field::Json(j) => SqlValue::Str(j.to_string()),
        Field::Date(d) => SqlValue::Date(*d),
        Field::Time(t) => SqlValue::Time(*t),
        Field::DateTime(dt) => SqlValue::DateTime(*dt),
        Field::TimestampTz(ts) => SqlValue::TimestampTz(*ts),
        // Durations are stored as nanosecond counts.
        Field::Duration(ns) => SqlValue::Int(*ns),
        Field::Array(items) => SqlValue::Str(
            serde_json::to_string(&items.iter().map(field_to_json_scalar).collect::<Vec<_>>())
                .unwrap_or_default(),
        ),
        Field::Row(fields) => SqlValue::Str(
            serde_json::to_string(&fields.iter().map(field_to_json_scalar).collect::<Vec<_>>())
                .unwrap_or_default(),
        ),
    }
}

fn field_to_json_scalar(field: &Field) -> serde_json::Value {
    match field {
        Field::Null | Field::Row(_) | Field::Array(_) => serde_json::Value::Null,
        other => serde_json::Value::String(format!("{:?}", other)),
    }
}

// ---------------------------------------------------------------------------
// SqlValue → Field (type-directed)
// ---------------------------------------------------------------------------

/// Convert a driver value into a typed Field using the discovered column
/// type. Text representations are parsed for temporal / decimal / json
/// columns.
pub fn sql_value_to_field(value: &SqlValue, column_type: &ColumnType) -> Field {
    use ColumnType::*;
    match value {
        SqlValue::Null => Field::Null,
        SqlValue::Bool(b) => Field::Bool(*b),
        SqlValue::Int(v) => match column_type {
            Int8 => Field::Int8(*v as i8),
            Int16 => Field::Int16(*v as i16),
            Int32 => Field::Int32(*v as i32),
            UInt8 => Field::UInt8(*v as u8),
            UInt16 => Field::UInt16(*v as u16),
            UInt32 => Field::UInt32(*v as u32),
            UInt64 => Field::UInt64(*v as u64),
            Float32 => Field::Float32(*v as f32),
            Float64 => Field::Float64(*v as f64),
            _ => Field::Int64(*v),
        },
        SqlValue::UInt(v) => Field::UInt64(*v),
        SqlValue::Float(v) => match column_type {
            Float32 => Field::Float32(*v as f32),
            _ => Field::Float64(*v),
        },
        SqlValue::Bytes(b) => match column_type {
            ColumnType::String => {
                Field::String(::std::string::String::from_utf8_lossy(b).to_string())
            }
            _ => Field::Bytes(b.clone()),
        },
        SqlValue::Date(d) => Field::Date(*d),
        SqlValue::Time(t) => Field::Time(*t),
        SqlValue::DateTime(dt) => Field::DateTime(*dt),
        SqlValue::TimestampTz(ts) => Field::TimestampTz(*ts),
        SqlValue::Str(s) => parse_text_to_field(s, column_type),
    }
}

/// Parse a text representation into a typed Field based on the column type.
pub fn parse_text_to_field(s: &str, column_type: &ColumnType) -> Field {
    use ColumnType::*;
    match column_type {
        Bool => match s.to_lowercase().as_str() {
            "true" | "t" | "1" | "yes" => Field::Bool(true),
            "false" | "f" | "0" | "no" => Field::Bool(false),
            _ => Field::String(s.to_string()),
        },
        Int8 => s
            .parse::<i8>()
            .map(Field::Int8)
            .unwrap_or(Field::String(s.to_string())),
        Int16 => s
            .parse::<i16>()
            .map(Field::Int16)
            .unwrap_or(Field::String(s.to_string())),
        Int32 => s
            .parse::<i32>()
            .map(Field::Int32)
            .unwrap_or(Field::String(s.to_string())),
        Int64 => s
            .parse::<i64>()
            .map(Field::Int64)
            .unwrap_or(Field::String(s.to_string())),
        UInt8 => s
            .parse::<u8>()
            .map(Field::UInt8)
            .unwrap_or(Field::String(s.to_string())),
        UInt16 => s
            .parse::<u16>()
            .map(Field::UInt16)
            .unwrap_or(Field::String(s.to_string())),
        UInt32 => s
            .parse::<u32>()
            .map(Field::UInt32)
            .unwrap_or(Field::String(s.to_string())),
        UInt64 => s
            .parse::<u64>()
            .map(Field::UInt64)
            .unwrap_or(Field::String(s.to_string())),
        Float32 => s
            .parse::<f32>()
            .map(Field::Float32)
            .unwrap_or(Field::String(s.to_string())),
        Float64 => s
            .parse::<f64>()
            .map(Field::Float64)
            .unwrap_or(Field::String(s.to_string())),
        Decimal { .. } => s
            .parse::<bigdecimal::BigDecimal>()
            .map(Field::Decimal)
            .unwrap_or(Field::String(s.to_string())),
        Json => serde_json::from_str::<serde_json::Value>(s)
            .map(Field::Json)
            .unwrap_or(Field::String(s.to_string())),
        Date => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(Field::Date)
            .unwrap_or(Field::String(s.to_string())),
        Time => NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
            .map(Field::Time)
            .unwrap_or(Field::String(s.to_string())),
        DateTime => parse_naive_datetime(s)
            .map(Field::DateTime)
            .unwrap_or(Field::String(s.to_string())),
        TimestampTz => chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|ts| Field::TimestampTz(ts.with_timezone(&Utc)))
            .or_else(|| parse_naive_datetime(s).map(|dt| Field::TimestampTz(dt.and_utc())))
            .unwrap_or(Field::String(s.to_string())),
        Duration => s
            .parse::<i64>()
            .map(Field::Duration)
            .unwrap_or(Field::String(s.to_string())),
        Array { .. } | Map { .. } => Field::String(s.to_string()),
        String | Bytes | Nullable(_) => Field::String(s.to_string()),
    }
}

fn parse_naive_datetime(s: &str) -> Option<NaiveDateTime> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt);
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
}

// ---------------------------------------------------------------------------
// mysql_async conversions
// ---------------------------------------------------------------------------

impl From<&SqlValue> for mysql_async::Value {
    fn from(value: &SqlValue) -> Self {
        use chrono::{Datelike, Timelike};
        match value {
            SqlValue::Null => mysql_async::Value::NULL,
            SqlValue::Bool(b) => mysql_async::Value::Int(*b as i64),
            SqlValue::Int(v) => mysql_async::Value::Int(*v),
            SqlValue::UInt(v) => mysql_async::Value::UInt(*v),
            SqlValue::Float(v) => mysql_async::Value::Double(*v),
            SqlValue::Str(s) => mysql_async::Value::Bytes(s.as_bytes().to_vec()),
            SqlValue::Bytes(b) => mysql_async::Value::Bytes(b.clone()),
            SqlValue::Date(d) => mysql_async::Value::Date(
                d.year().unsigned_abs() as u16,
                d.month() as u8,
                d.day() as u8,
                0,
                0,
                0,
                0,
            ),
            SqlValue::Time(t) => mysql_async::Value::Time(
                false,
                0,
                t.hour() as u8,
                t.minute() as u8,
                t.second() as u8,
                t.nanosecond() / 1000,
            ),
            SqlValue::DateTime(dt) => mysql_async::Value::Date(
                dt.year().unsigned_abs() as u16,
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000,
            ),
            SqlValue::TimestampTz(ts) => {
                let dt = ts.naive_utc();
                mysql_async::Value::Date(
                    dt.year().unsigned_abs() as u16,
                    dt.month() as u8,
                    dt.day() as u8,
                    dt.hour() as u8,
                    dt.minute() as u8,
                    dt.second() as u8,
                    dt.nanosecond() / 1000,
                )
            }
        }
    }
}

/// Convert a raw mysql_async value into a driver-neutral value.
pub fn mysql_value_to_sql(value: &mysql_async::Value) -> SqlValue {
    match value {
        mysql_async::Value::NULL => SqlValue::Null,
        mysql_async::Value::Int(v) => SqlValue::Int(*v),
        mysql_async::Value::UInt(v) => SqlValue::UInt(*v),
        mysql_async::Value::Float(v) => SqlValue::Float(*v as f64),
        mysql_async::Value::Double(v) => SqlValue::Float(*v),
        mysql_async::Value::Bytes(b) => SqlValue::Bytes(b.clone()),
        mysql_async::Value::Date(y, m, d, h, mi, s, us) => {
            match NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32) {
                Some(date) => {
                    let time = NaiveTime::from_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us)
                        .unwrap_or_default();
                    if (*h, *mi, *s, *us) == (0, 0, 0, 0) {
                        SqlValue::Date(date)
                    } else {
                        SqlValue::DateTime(date.and_time(time))
                    }
                }
                None => SqlValue::Null,
            }
        }
        mysql_async::Value::Time(neg, d, h, m, s, us) => {
            let total_secs =
                (*d as u64) * 86_400 + (*h as u64) * 3_600 + (*m as u64) * 60 + *s as u64;
            if *neg {
                // Negative durations are stored as nanosecond counts.
                SqlValue::Int(-(total_secs as i64) * 1_000_000_000 - (*us as i64) * 1_000)
            } else {
                SqlValue::Time(
                    NaiveTime::from_hms_micro_opt(*h as u32, *m as u32, *s as u32, *us)
                        .unwrap_or_default(),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_roundtrip_through_sql_value() {
        let field = Field::Int32(42);
        let sql = field_to_sql_value(&field);
        assert_eq!(sql, SqlValue::Int(42));
        let back = sql_value_to_field(&sql, &ColumnType::Int32);
        assert_eq!(back, Field::Int32(42));
    }

    #[test]
    fn test_text_parsing_temporal_decimal_json() {
        assert_eq!(
            parse_text_to_field("2024-05-06", &ColumnType::Date),
            Field::Date(NaiveDate::from_ymd_opt(2024, 5, 6).unwrap())
        );
        assert_eq!(
            parse_text_to_field("2024-05-06 07:08:09", &ColumnType::DateTime),
            Field::DateTime(
                NaiveDate::from_ymd_opt(2024, 5, 6)
                    .unwrap()
                    .and_hms_opt(7, 8, 9)
                    .unwrap()
            )
        );
        assert!(matches!(
            parse_text_to_field("{\"a\":1}", &ColumnType::Json),
            Field::Json(_)
        ));
        assert!(matches!(
            parse_text_to_field(
                "3.14",
                &ColumnType::Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            Field::Decimal(_)
        ));
    }

    #[test]
    fn test_mysql_value_datetime() {
        let sql = mysql_value_to_sql(&mysql_async::Value::Date(2024, 5, 6, 7, 8, 9, 0));
        match sql {
            SqlValue::DateTime(dt) => {
                assert_eq!(dt.to_string(), "2024-05-06 07:08:09");
            }
            other => panic!("expected DateTime, got {:?}", other),
        }
    }
}
