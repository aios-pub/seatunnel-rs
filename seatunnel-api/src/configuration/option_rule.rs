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

use std::collections::{HashMap, HashSet};

/// Validation rules for connector configuration.
///
/// Mirrors Java's `OptionRule` — declarative validation for connector options.
/// Supports required, optional, exclusive, conditional, and bundled rules.
#[derive(Debug, Clone, Default)]
pub struct OptionRule {
    required: HashSet<String>,
    optional: HashSet<String>,
    exclusive: Vec<HashSet<String>>,
    conditional: Vec<ConditionalRule>,
    bundled: Vec<HashSet<String>>,
}

#[derive(Debug, Clone)]
struct ConditionalRule {
    trigger_key: String,
    trigger_value: String,
    then_required: HashSet<String>,
}

impl OptionRule {
    pub fn new() -> Self {
        OptionRule::default()
    }

    /// Add required options.
    pub fn required(mut self, keys: impl IntoIterator<Item = String>) -> Self {
        for key in keys {
            self.required.insert(key);
        }
        self
    }

    /// Add optional options.
    pub fn optional(mut self, keys: impl IntoIterator<Item = String>) -> Self {
        for key in keys {
            self.optional.insert(key);
        }
        self
    }

    /// Add exclusive options (only one of these can be set).
    pub fn exclusive(mut self, groups: impl IntoIterator<Item = Vec<String>>) -> Self {
        for group in groups {
            self.exclusive.push(group.into_iter().collect());
        }
        self
    }

    /// Add conditional rule: if trigger_key == trigger_value, then these keys become required.
    pub fn conditional(
        mut self,
        trigger_key: impl Into<String>,
        trigger_value: impl Into<String>,
        then_required: impl IntoIterator<Item = String>,
    ) -> Self {
        self.conditional.push(ConditionalRule {
            trigger_key: trigger_key.into(),
            trigger_value: trigger_value.into(),
            then_required: then_required.into_iter().collect(),
        });
        self
    }

    /// Add bundled options (all or none must be set).
    pub fn bundled(mut self, groups: impl IntoIterator<Item = Vec<String>>) -> Self {
        for group in groups {
            self.bundled.push(group.into_iter().collect());
        }
        self
    }

    /// Validate a config against this rule.
    pub fn validate(&self, config: &super::ReadonlyConfig) -> Result<(), String> {
        // Check required options
        for key in &self.required {
            if !config.contains(key) {
                return Err(format!("Missing required option: {}", key));
            }
        }

        // Check exclusive options
        for group in &self.exclusive {
            let count = group.iter().filter(|k| config.contains(k)).count();
            if count > 1 {
                return Err(format!(
                    "Exclusive options cannot be set together: {:?}",
                    group
                ));
            }
            if count == 0 {
                return Err(format!(
                    "At least one of exclusive options must be set: {:?}",
                    group
                ));
            }
        }

        // Check conditional requirements
        for rule in &self.conditional {
            if config
                .get_string(&rule.trigger_key)
                .as_ref()
                .map(|v| v == &rule.trigger_value)
                .unwrap_or(false)
            {
                for key in &rule.then_required {
                    if !config.contains(key) {
                        return Err(format!(
                            "Option '{}' is required when '{}' = '{}'",
                            key, rule.trigger_key, rule.trigger_value
                        ));
                    }
                }
            }
        }

        // Check bundled options
        for group in &self.bundled {
            let present: Vec<&String> = group.iter().filter(|k| config.contains(k)).collect();
            if !present.is_empty() && present.len() != group.len() {
                return Err(format!(
                    "Bundled options must all be set or all be absent: {:?}",
                    group
                ));
            }
        }

        Ok(())
    }
}
