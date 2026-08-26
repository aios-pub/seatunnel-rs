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

//! Data types for rows and fields.
//!
//! This module defines the core data types used throughout SeaTunnel:
//! - [`ColumnType`] — SQL column type system (对标 sea-orm DataType)
//! - [`Field`] — Single field value (对标 sea-orm Value)
//! - [`Row`] — A row of data (fixed array of fields)
//! - [`RowKind`] — Change type (INSERT / UPDATE / DELETE)

mod column_type;
mod field;
#[allow(clippy::module_inception)] // public path `row::row` is part of the API
mod row;
mod row_kind;

pub use column_type::ColumnType;
pub use field::Field;
pub use row::{Row, RowBuilder};
pub use row_kind::RowKind;
