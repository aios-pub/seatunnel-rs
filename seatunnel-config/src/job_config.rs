/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::collections::HashMap;
use std::error::Error;

use crate::parser::ParsedConfig;
use seatunnel_api::{ColumnDef, ColumnType, TableSchema};
use serde_json::Value;

/// Top-level job configuration.
pub struct JobConfig {
    pub env: HashMap<String, Value>,
    pub sources: Vec<SourceConfig>,
    pub sinks: Vec<SinkConfig>,
    pub transforms: Vec<TransformConfig>,
}

impl JobConfig {
    pub fn from_parsed(parsed: ParsedConfig) -> Result<Self, Box<dyn Error>> {
        let env = value_to_map(parsed.env)?;
        let sources = parsed
            .sources
            .iter()
            .map(|v| SourceConfig::from_value(v.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let sinks = parsed
            .sinks
            .iter()
            .map(|v| SinkConfig::from_value(v.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let transforms = parsed
            .transforms
            .iter()
            .map(|v| TransformConfig::from_value(v.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(JobConfig {
            env,
            sources,
            sinks,
            transforms,
        })
    }

    pub fn parallelism(&self) -> usize {
        self.env
            .get("parallelism")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(1)
    }

    pub fn job_mode(&self) -> JobMode {
        self.env
            .get("job_mode")
            .and_then(|v| v.as_str())
            .and_then(|s| match s {
                "STREAMING" | "streaming" => Some(JobMode::Streaming),
                "BATCH" | "batch" => Some(JobMode::Batch),
                _ => None,
            })
            .unwrap_or(JobMode::Batch)
    }

    pub fn checkpoint_interval(&self) -> u64 {
        self.env
            .get("checkpoint_interval")
            .and_then(|v| v.as_u64())
            .unwrap_or(60_000)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JobMode {
    Batch,
    Streaming,
}

#[derive(Debug)]
pub struct SourceConfig {
    pub factory_identifier: String,
    pub options: HashMap<String, Value>,
    pub parallelism: usize,
    pub plugin_output: Option<String>,
    pub schema: Option<SchemaConfig>,
}

impl SourceConfig {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn Error>> {
        let obj = value.as_object().ok_or("Source must be an object")?;
        let mut factory_identifier = String::new();
        let mut parallelism = 1;
        let mut plugin_output: Option<String> = None;
        let mut schema: Option<SchemaConfig> = None;
        let mut options = HashMap::new();

        for (k, v) in obj {
            match k.as_str() {
                "parallelism" => parallelism = v.as_u64().unwrap_or(1) as usize,
                "plugin_output" => plugin_output = v.as_str().map(String::from),
                "schema" => schema = SchemaConfig::from_value(v.clone()).ok(),
                "plugin_name" => {
                    factory_identifier = v.as_str().map(String::from).unwrap_or_default()
                }
                _ => {
                    let _ = options.insert(k.clone(), v.clone());
                }
            }
        }

        Ok(SourceConfig {
            factory_identifier,
            options,
            parallelism,
            plugin_output,
            schema,
        })
    }
}

#[derive(Debug)]
pub struct SinkConfig {
    pub factory_identifier: String,
    pub options: HashMap<String, Value>,
    pub parallelism: usize,
    pub plugin_input: Option<String>,
}

impl SinkConfig {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn Error>> {
        let obj = value.as_object().ok_or("Sink must be an object")?;
        let mut factory_identifier = String::new();
        let mut parallelism = 1;
        let mut plugin_input: Option<String> = None;
        let mut options = HashMap::new();

        for (k, v) in obj {
            match k.as_str() {
                "parallelism" => parallelism = v.as_u64().unwrap_or(1) as usize,
                "plugin_input" => plugin_input = v.as_str().map(String::from),
                "plugin_name" => {
                    factory_identifier = v.as_str().map(String::from).unwrap_or_default()
                }
                _ => {
                    let _ = options.insert(k.clone(), v.clone());
                }
            }
        }

        Ok(SinkConfig {
            factory_identifier,
            options,
            parallelism,
            plugin_input,
        })
    }
}

#[derive(Debug)]
pub struct TransformConfig {
    pub factory_identifier: String,
    pub options: HashMap<String, Value>,
}

impl TransformConfig {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn Error>> {
        let obj = value.as_object().ok_or("Transform must be an object")?;
        let mut factory_identifier = String::new();
        let mut options = HashMap::new();

        for (k, v) in obj {
            match k.as_str() {
                "plugin_name" => {
                    factory_identifier = v.as_str().map(String::from).unwrap_or_default()
                }
                _ => {
                    let _ = options.insert(k.clone(), v.clone());
                }
            }
        }

        Ok(TransformConfig {
            factory_identifier,
            options,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SchemaConfig {
    pub fields: Vec<(String, String)>,
}

impl SchemaConfig {
    pub fn from_value(value: Value) -> Result<Self, Box<dyn Error>> {
        let fields: Vec<(String, String)> = match &value {
            Value::Object(obj) => {
                let mut result = Vec::new();
                if let Some(fields_obj) = obj.get("fields") {
                    if let Some(fields_map) = fields_obj.as_object() {
                        for (name, type_val) in fields_map {
                            let type_str = type_val.as_str().unwrap_or("string").to_string();
                            result.push((name.clone(), type_str));
                        }
                    }
                }
                result
            }
            _ => Vec::new(),
        };
        Ok(SchemaConfig { fields })
    }

    pub fn to_table_schema(&self, table_name: &str) -> TableSchema {
        let columns: Vec<ColumnDef> = self
            .fields
            .iter()
            .map(|(name, type_str)| {
                let col_type = string_to_column_type(type_str);
                ColumnDef::new(name.clone(), col_type)
            })
            .collect();
        TableSchema::new(table_name, columns)
    }
}

fn string_to_column_type(s: &str) -> ColumnType {
    match s.to_lowercase().as_str() {
        "bool" | "boolean" => ColumnType::Bool,
        "tinyint" => ColumnType::Int8,
        "smallint" => ColumnType::Int16,
        "int" | "integer" => ColumnType::Int32,
        "bigint" => ColumnType::Int64,
        "float" => ColumnType::Float32,
        "double" => ColumnType::Float64,
        "string" | "varchar" | "text" => ColumnType::String,
        "bytes" | "binary" => ColumnType::Bytes,
        "json" => ColumnType::Json,
        "date" => ColumnType::Date,
        "time" => ColumnType::Time,
        "datetime" => ColumnType::DateTime,
        "timestamp" => ColumnType::TimestampTz,
        _ => ColumnType::String,
    }
}

fn value_to_map(value: Option<Value>) -> Result<HashMap<String, Value>, Box<dyn Error>> {
    match value {
        Some(Value::Object(obj)) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k, v);
            }
            Ok(map)
        }
        Some(_) => Err("env must be an object".into()),
        None => Ok(HashMap::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_mode() {
        let env = create_env("job_mode", "streaming");
        let config = JobConfig {
            env,
            sources: vec![],
            sinks: vec![],
            transforms: vec![],
        };
        assert_eq!(config.job_mode(), JobMode::Streaming);
    }

    #[test]
    fn test_parallelism() {
        let env = create_env("parallelism", "4");
        let config = JobConfig {
            env,
            sources: vec![],
            sinks: vec![],
            transforms: vec![],
        };
        assert_eq!(config.parallelism(), 4);
    }

    #[test]
    fn test_source_config() {
        let config = serde_json::json!({
            "plugin_name": "FakeSource",
            "parallelism": 2,
            "rows": 100,
            "fields": {"id": "int", "name": "string"}
        });
        let source = SourceConfig::from_value(config).unwrap();
        assert_eq!(source.factory_identifier, "FakeSource");
        assert_eq!(source.parallelism, 2);
        assert_eq!(source.options.get("rows").unwrap().as_u64(), Some(100));
    }

    #[test]
    fn test_schema_config() {
        let config = serde_json::json!({
            "fields": {"id": "int", "name": "string", "ts": "timestamp"}
        });
        let schema = SchemaConfig::from_value(config).unwrap();
        assert_eq!(schema.fields.len(), 3);
        assert!(schema.fields.iter().any(|(n, t)| n == "id" && t == "int"));
        assert!(schema
            .fields
            .iter()
            .any(|(n, t)| n == "ts" && t == "timestamp"));
    }

    #[test]
    fn test_transform_config() {
        let config = serde_json::json!({
            "plugin_name": "Filter",
            "expr": "age > 18"
        });
        let transform = TransformConfig::from_value(config).unwrap();
        assert_eq!(transform.factory_identifier, "Filter");
        assert_eq!(
            transform.options.get("expr").unwrap().as_str(),
            Some("age > 18")
        );
    }

    fn create_env(key: &str, value: &str) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        map.insert(
            key.to_string(),
            value
                .parse::<u64>()
                .map(Value::from)
                .unwrap_or(Value::String(value.to_string())),
        );
        map
    }
}
