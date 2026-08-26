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

//! Table schema definitions and database dialect system.

mod column_def;
mod dialect;
mod discoverer;
mod schema_event;
mod table_schema;

pub use column_def::ColumnDef;
pub use dialect::{DatabaseDialect, MySqlDialect, PostgresDialect, SchemaDiscovery, TiDbDialect};
pub use discoverer::SchemaDiscoverer;
pub use schema_event::{translate_positional, SchemaChange, SchemaChangeEvent, SchemaChangeError};
pub use table_schema::TableSchema;
