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

//! Core API definitions for Apache SeaTunnel Rust.
//!
//! This crate defines the fundamental types and traits used throughout the system:
//! - [`row`] — Row data types (Field, Row, RowKind, ColumnType)
//! - [`schema`] — Table schema discovery and type mapping
//! - [`source`] — Source connector interfaces
//! - [`sink`] — Sink connector interfaces
//! - [`transform`] — Transform connector interfaces
//! - [`configuration`] — Configuration system (ConfigOption, OptionRule)
//! - [`factory`] — Plugin factory and SPI registration
//! - [`execution`] — Execution mode (Local/Cluster) and Engine

pub mod configuration;
pub mod execution;
pub mod factory;
pub mod row;
pub mod schema;
pub mod sink;
pub mod source;
pub mod transform;

pub use configuration::{ConfigOption, OptionRule, ReadonlyConfig};
pub use execution::{Engine, ExecutionMode};
pub use factory::{Factory, FactoryContext};
pub use row::{ColumnType, Field, Row, RowKind};
pub use schema::{
    ColumnDef, DatabaseDialect, SchemaChange, SchemaChangeEvent, SchemaChangeError,
    SchemaDiscoverer, TableSchema,
};
pub use sink::{Sink, SinkCommitter, SinkWriter};
pub use source::{Source, SourceReader, SourceSplit, SourceSplitEnumerator};
pub use transform::Transform;
