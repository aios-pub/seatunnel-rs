/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use serde_json::Value;
use std::error::Error;

pub use crate::hocon;

/// Supported config file formats.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConfigFormat {
    YAML,
    TOML,
    HOCON,
}

pub struct ParsedConfig {
    pub env: Option<Value>,
    pub sources: Vec<Value>,
    pub sinks: Vec<Value>,
    pub transforms: Vec<Value>,
}

pub fn parse_config_file(
    content: &str,
    format: ConfigFormat,
) -> Result<ParsedConfig, Box<dyn Error>> {
    match format {
        ConfigFormat::YAML => parse_yaml(content),
        ConfigFormat::TOML => parse_toml(content),
        ConfigFormat::HOCON => parse_hocon_format(content),
    }
}

fn parse_yaml(content: &str) -> Result<ParsedConfig, Box<dyn Error>> {
    let value: Value = serde_yaml::from_str(content)?;
    extract_sections(value)
}

fn parse_toml(content: &str) -> Result<ParsedConfig, Box<dyn Error>> {
    let table: toml::Table = toml::from_str(content)?;
    let value: Value = serde_json::to_value(table)?;
    extract_sections(value)
}

fn parse_hocon_format(content: &str) -> Result<ParsedConfig, Box<dyn Error>> {
    let value = hocon::parse_hocon(content).map_err(|e| format!("HOCON parse error: {}", e))?;
    extract_sections(value)
}

fn extract_sections(value: Value) -> Result<ParsedConfig, Box<dyn Error>> {
    let obj = value.as_object().ok_or("Config must be an object")?;

    let env = obj.get("env").cloned();

    let sources = match obj.get("source") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(Value::Object(obj2)) => {
            let mut arr = Vec::new();
            for (k, v) in obj2 {
                let mut item = serde_json::Map::new();
                item.insert(k.clone(), v.clone());
                arr.push(Value::Object(item));
            }
            arr
        }
        Some(v) => vec![v.clone()],
        None => Vec::new(),
    };

    let sinks = match obj.get("sink") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(Value::Object(obj2)) => {
            let mut arr = Vec::new();
            for (k, v) in obj2 {
                let mut item = serde_json::Map::new();
                item.insert(k.clone(), v.clone());
                arr.push(Value::Object(item));
            }
            arr
        }
        Some(v) => vec![v.clone()],
        None => Vec::new(),
    };

    let transforms = match obj.get("transform") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(Value::Object(obj2)) => {
            let mut arr = Vec::new();
            for (k, v) in obj2 {
                let mut item = serde_json::Map::new();
                item.insert(k.clone(), v.clone());
                arr.push(Value::Object(item));
            }
            arr
        }
        Some(v) => vec![v.clone()],
        None => Vec::new(),
    };

    Ok(ParsedConfig {
        env,
        sources,
        sinks,
        transforms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hocon_env() {
        let content = "env { job.name = \"demo\" parallelism.default = 4 }";
        let result = parse_config_file(content, ConfigFormat::HOCON).unwrap();
        assert!(result.env.is_some());
        let env = result.env.unwrap();
        assert_eq!(env["job"]["name"], "demo");
        assert_eq!(env["parallelism"]["default"], 4);
    }

    #[test]
    fn test_parse_hocon_source() {
        let content = "source { kafka { topic = \"t1\" format = \"json\" } }";
        let result = parse_config_file(content, ConfigFormat::HOCON).unwrap();
        assert_eq!(result.sources.len(), 1);
        let src = &result.sources[0];
        assert_eq!(src["kafka"]["topic"], "t1");
    }
}
