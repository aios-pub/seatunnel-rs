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

//! Plugin factory system for connector discovery.

use crate::configuration::ReadonlyConfig;

/// Context passed to factory create methods.
#[derive(Debug, Clone)]
pub struct FactoryContext {
    pub factory_identifier: String,
    pub options: ReadonlyConfig,
}

/// Base trait for all factory implementations.
pub trait Factory: Send + Sync {
    /// Returns the unique identifier for this factory.
    fn factory_identifier(&self) -> &str;

    /// Get the factory category (source, sink, transform).
    fn category(&self) -> FactoryCategory {
        FactoryCategory::Source
    }
}

/// Category of a factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryCategory {
    Source,
    Sink,
    Transform,
    All,
}

/// Registry for discovered factories.
pub struct FactoryRegistry {
    factories: Vec<Box<dyn Factory>>,
}

impl Default for FactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FactoryRegistry {
    pub fn new() -> Self {
        FactoryRegistry {
            factories: Vec::new(),
        }
    }

    pub fn register(&mut self, factory: impl Factory + 'static) {
        self.factories.push(Box::new(factory));
    }

    pub fn find_by_identifier(&self, identifier: &str) -> Option<&dyn Factory> {
        self.factories
            .iter()
            .find(|f| f.factory_identifier() == identifier)
            .map(|f| f.as_ref())
    }

    pub fn find_by_category(&self, category: FactoryCategory) -> Vec<&dyn Factory> {
        self.factories
            .iter()
            .filter(|f| match category {
                FactoryCategory::All => true,
                FactoryCategory::Source => f.category() == FactoryCategory::Source,
                FactoryCategory::Sink => f.category() == FactoryCategory::Sink,
                FactoryCategory::Transform => f.category() == FactoryCategory::Transform,
            })
            .map(|f| f.as_ref())
            .collect()
    }

    pub fn all(&self) -> &[Box<dyn Factory>] {
        &self.factories
    }
}
