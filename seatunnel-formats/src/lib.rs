/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Data format serializers/deserializers for SeaTunnel.
//!
//! Supports: JSON, Text, Canal JSON, Debezium JSON, Compatible Debezium JSON,
//! Compatible Kafka Connect JSON, OGG JSON, Maxwell JSON, Avro, Protobuf, Native.

use seatunnel_api::{ColumnType, Row, TableSchema};
use serde_json::Value;
use std::error::Error;

pub mod avro;
pub mod canal_client_json;
pub mod canal_json;
pub mod compatible_debezium_json;
pub mod compatible_kafka_connect_json;
pub mod debezium_json;
pub mod json;
pub mod maxwell_json;
pub mod native;
pub mod ogg_json;
pub mod protobuf;
pub mod text;

/// Supported message formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageFormat {
    Json,
    Text,
    CanalJson,
    CanalClientJson,
    DebeziumJson,
    CompatibleDebeziumJson,
    CompatibleKafkaConnectJson,
    OggJson,
    MaxwellJson,
    Avro,
    Protobuf,
    Native,
}

impl MessageFormat {
    // Kept as an inherent method for backward-compatible public API;
    // renaming or removing it would break downstream callers.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "json" => Some(MessageFormat::Json),
            "text" => Some(MessageFormat::Text),
            "canal_json" | "canal-json" => Some(MessageFormat::CanalJson),
            "canal_client_json" | "canal-client-json" | "canal_client" | "canal-client" => {
                Some(MessageFormat::CanalClientJson)
            }
            "debezium_json" | "debezium-json" => Some(MessageFormat::DebeziumJson),
            "compatible_debezium_json" | "compatible-debezium-json" => {
                Some(MessageFormat::CompatibleDebeziumJson)
            }
            "compatible_kafka_connect_json" | "compatible-kafka-connect-json" => {
                Some(MessageFormat::CompatibleKafkaConnectJson)
            }
            "ogg_json" | "ogg-json" => Some(MessageFormat::OggJson),
            "maxwell_json" | "maxwell-json" => Some(MessageFormat::MaxwellJson),
            "avro" => Some(MessageFormat::Avro),
            "protobuf" => Some(MessageFormat::Protobuf),
            "native" => Some(MessageFormat::Native),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            MessageFormat::Json => "JSON",
            MessageFormat::Text => "TEXT",
            MessageFormat::CanalJson => "CANAL_JSON",
            MessageFormat::CanalClientJson => "CANAL_CLIENT_JSON",
            MessageFormat::DebeziumJson => "DEBEZIUM_JSON",
            MessageFormat::CompatibleDebeziumJson => "COMPATIBLE_DEBEZIUM_JSON",
            MessageFormat::CompatibleKafkaConnectJson => "COMPATIBLE_KAFKA_CONNECT_JSON",
            MessageFormat::OggJson => "OGG_JSON",
            MessageFormat::MaxwellJson => "MAXWELL_JSON",
            MessageFormat::Avro => "AVRO",
            MessageFormat::Protobuf => "PROTOBUF",
            MessageFormat::Native => "NATIVE",
        }
    }
}

/// Deserialize bytes into a Row using the specified format.
/// For CDC formats that emit multiple rows (e.g. UPDATE), takes the first row.
pub fn deserialize(
    format: MessageFormat,
    bytes: &[u8],
    schema: &TableSchema,
) -> Result<Row, Box<dyn Error>> {
    let rows = match format {
        MessageFormat::Json => json::deserialize(bytes, schema)?,
        MessageFormat::Text => text::deserialize(bytes, schema)?,
        MessageFormat::CanalJson => canal_json::deserialize(bytes, schema)?,
        MessageFormat::CanalClientJson => canal_client_json::deserialize(bytes, schema)?,
        MessageFormat::DebeziumJson => debezium_json::deserialize(bytes, schema)?,
        MessageFormat::CompatibleDebeziumJson => {
            compatible_debezium_json::deserialize(bytes, schema)?
        }
        MessageFormat::CompatibleKafkaConnectJson => {
            compatible_kafka_connect_json::deserialize(bytes, schema)?
        }
        MessageFormat::OggJson => ogg_json::deserialize(bytes, schema)?,
        MessageFormat::MaxwellJson => maxwell_json::deserialize(bytes, schema)?,
        MessageFormat::Avro => avro::deserialize(bytes, schema)?,
        MessageFormat::Protobuf => protobuf::deserialize(bytes, schema)?,
        MessageFormat::Native => native::deserialize(bytes, schema)?,
    };
    rows.into_iter()
        .next()
        .ok_or("Empty result from format deserializer".into())
}

/// Deserialize bytes into all Rows (useful for CDC formats with UPDATE_BEFORE/UPDATE_AFTER).
pub fn deserialize_all(
    format: MessageFormat,
    bytes: &[u8],
    schema: &TableSchema,
) -> Result<Vec<Row>, Box<dyn Error>> {
    match format {
        MessageFormat::Json => json::deserialize(bytes, schema),
        MessageFormat::Text => text::deserialize(bytes, schema),
        MessageFormat::CanalJson => canal_json::deserialize(bytes, schema),
        MessageFormat::CanalClientJson => canal_client_json::deserialize(bytes, schema),
        MessageFormat::DebeziumJson => debezium_json::deserialize(bytes, schema),
        MessageFormat::CompatibleDebeziumJson => {
            compatible_debezium_json::deserialize(bytes, schema)
        }
        MessageFormat::CompatibleKafkaConnectJson => {
            compatible_kafka_connect_json::deserialize(bytes, schema)
        }
        MessageFormat::OggJson => ogg_json::deserialize(bytes, schema),
        MessageFormat::MaxwellJson => maxwell_json::deserialize(bytes, schema),
        MessageFormat::Avro => avro::deserialize(bytes, schema),
        MessageFormat::Protobuf => protobuf::deserialize(bytes, schema),
        MessageFormat::Native => native::deserialize(bytes, schema),
    }
}

/// Serialize a Row into bytes using the specified format.
pub fn serialize(
    format: MessageFormat,
    schema: &TableSchema,
    row: &Row,
) -> Result<Vec<u8>, Box<dyn Error>> {
    match format {
        MessageFormat::Json => json::serialize(schema, row),
        MessageFormat::Text => text::serialize(schema, row),
        MessageFormat::CanalJson => canal_json::serialize(schema, row),
        MessageFormat::CanalClientJson => canal_client_json::serialize(schema, row),
        MessageFormat::DebeziumJson => debezium_json::serialize(schema, row),
        MessageFormat::CompatibleDebeziumJson => compatible_debezium_json::serialize(schema, row),
        MessageFormat::CompatibleKafkaConnectJson => {
            compatible_kafka_connect_json::serialize(schema, row)
        }
        MessageFormat::OggJson => ogg_json::serialize(schema, row),
        MessageFormat::MaxwellJson => maxwell_json::serialize(schema, row),
        MessageFormat::Avro => avro::serialize(schema, row),
        MessageFormat::Protobuf => protobuf::serialize(schema, row),
        MessageFormat::Native => native::serialize(schema, row),
    }
}

/// Convert a serde_json Value to a Row given a schema.
pub fn value_to_row(value: &Value, schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        let val = value.get(&col.name);
        row.set(i, json_value_to_field(val, &col.column_type)?);
    }
    Ok(row)
}

fn json_value_to_field(
    value: Option<&Value>,
    _col_type: &ColumnType,
) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    let value = match value {
        Some(v) => v,
        None => return Ok(seatunnel_api::Field::Null),
    };
    match value {
        Value::Bool(b) => Ok(seatunnel_api::Field::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(seatunnel_api::Field::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(seatunnel_api::Field::UInt64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(seatunnel_api::Field::Float64(f))
            } else {
                Ok(seatunnel_api::Field::Null)
            }
        }
        Value::String(s) => Ok(seatunnel_api::Field::String(s.clone())),
        Value::Array(arr) => {
            let fields: Vec<seatunnel_api::Field> = arr
                .iter()
                .map(|v| json_value_to_field(Some(v), &ColumnType::String))
                .collect::<Result<_, _>>()?;
            Ok(seatunnel_api::Field::Array(fields))
        }
        Value::Object(_) => Ok(seatunnel_api::Field::Json(value.clone())),
        Value::Null => Ok(seatunnel_api::Field::Null),
    }
}
