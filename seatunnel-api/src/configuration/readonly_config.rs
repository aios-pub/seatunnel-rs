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

use std::collections::HashMap;

/// Read-only configuration wrapper.
///
/// Mirrors Java's `ReadonlyConfig` — an immutable view over configuration data
/// with type-safe accessors.
#[derive(Debug, Clone)]
pub struct ReadonlyConfig {
    data: HashMap<String, String>,
}

impl ReadonlyConfig {
    /// Create from a map of key-value pairs.
    pub fn from_map(map: HashMap<String, String>) -> Self {
        ReadonlyConfig { data: map }
    }

    /// Create from a serde_json Value (for YAML/JSON config parsing).
    pub fn from_json(value: serde_json::Value) -> Self {
        let mut map = HashMap::new();
        if let serde_json::Value::Object(obj) = value {
            for (k, v) in obj {
                map.insert(k, v.to_string());
            }
        }
        ReadonlyConfig { data: map }
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Get a string value.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|s| s.as_str())
    }

    /// Get an integer value.
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.data.get(key).and_then(|s| s.parse::<i64>().ok())
    }

    /// Get a boolean value.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.data.get(key).and_then(|s| s.parse::<bool>().ok())
    }

    /// Get all entries.
    pub fn entries(&self) -> impl Iterator<Item = (&String, &String)> {
        self.data.iter()
    }
}

impl From<HashMap<String, String>> for ReadonlyConfig {
    fn from(data: HashMap<String, String>) -> Self {
        ReadonlyConfig::from_map(data)
    }
}

impl std::fmt::Display for ReadonlyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut first = true;
        for (k, v) in &self.data {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", k, v)?;
            first = false;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_map() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "42".to_string());

        let config = ReadonlyConfig::from_map(map);
        assert_eq!(config.get_string("key1"), Some("value1"));
        assert_eq!(config.get_int("key2"), Some(42));
        assert!(!config.contains("missing"));
    }

    #[test]
    fn test_get_bool() {
        let mut map = HashMap::new();
        map.insert("enabled".to_string(), "true".to_string());
        map.insert("disabled".to_string(), "false".to_string());

        let config = ReadonlyConfig::from_map(map);
        assert_eq!(config.get_bool("enabled"), Some(true));
        assert_eq!(config.get_bool("disabled"), Some(false));
    }
}
