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

/// A typed configuration option key.
#[derive(Debug, Clone)]
pub struct ConfigOption<T: Clone> {
    pub key: String,
    pub default_value: Option<T>,
    pub description: String,
    pub fallback_keys: Vec<String>,
}

impl<T: Clone + Default> ConfigOption<T> {
    pub fn new(key: impl Into<String>, description: impl Into<String>) -> Self {
        ConfigOption {
            key: key.into(),
            default_value: Some(T::default()),
            description: description.into(),
            fallback_keys: Vec::new(),
        }
    }

    pub fn default_value(mut self, value: T) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn fallback_key(mut self, key: impl Into<String>) -> Self {
        self.fallback_keys.push(key.into());
        self
    }
}

/// A map configuration value.
#[derive(Debug, Clone, Default)]
pub struct ConfigMap(pub std::collections::HashMap<String, String>);

impl ConfigMap {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }
    pub fn insert(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }
}
