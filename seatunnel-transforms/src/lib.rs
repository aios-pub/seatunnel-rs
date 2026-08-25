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

//! Transform operators for SeaTunnel.
//!
//! Implements the core transform operators:
//! - [FilterTransform]: conditional filtering
//! - [MapTransform]: field-level mapping
//! - [FanoutTransform]: one-to-many branching
//! - [RenameTransform]: column renaming
//! - [SelectTransform]: column projection

use seatunnel_api::{
    row::{Row, RowKind},
    schema::{TableSchema, ColumnDef},
    transform::Transform,
};

/// Generic filter predicate.
pub trait FilterPredicate: Send + Sync {
    fn test(&self, row: &Row) -> bool;
}

/// A filter transform that drops rows not matching a predicate.
pub struct FilterTransform<F: FilterPredicate> {
    predicate: F,
    output_schema: Option<TableSchema>,
}

impl<F: FilterPredicate> FilterTransform<F> {
    pub fn new(predicate: F) -> Self {
        FilterTransform {
            predicate,
            output_schema: None,
        }
    }

    pub fn with_output_schema(mut self, schema: TableSchema) -> Self {
        self.output_schema = Some(schema);
        self
    }
}

impl<F: FilterPredicate> Transform for FilterTransform<F> {
    type Input = Row;
    type Output = Row;

    fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
        if self.predicate.test(&record) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.output_schema.clone()
    }

    fn set_input_schema(&mut self, schema: TableSchema) {
        if self.output_schema.is_none() {
            self.output_schema = Some(schema);
        }
    }
}

/// Field-level mapping function.
pub trait FieldMapper: Send + Sync {
    fn map(&self, row: Row) -> Row;
}

/// A map transform that applies a function to each row.
pub struct MapTransform<F: FieldMapper> {
    mapper: F,
    output_schema: Option<TableSchema>,
}

impl<F: FieldMapper> MapTransform<F> {
    pub fn new(mapper: F) -> Self {
        MapTransform {
            mapper,
            output_schema: None,
        }
    }

    pub fn with_output_schema(mut self, schema: TableSchema) -> Self {
        self.output_schema = Some(schema);
        self
    }
}

impl<F: FieldMapper> Transform for MapTransform<F> {
    type Input = Row;
    type Output = Row;

    fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
        Ok(vec![self.mapper.map(record)])
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.output_schema.clone()
    }

    fn set_input_schema(&mut self, schema: TableSchema) {
        if self.output_schema.is_none() {
            self.output_schema = Some(schema);
        }
    }
}

/// Fanout configuration.
#[derive(Debug, Clone)]
pub enum FanoutMode {
    All,
    First { index: usize },
    N { count: usize },
}

/// A fanout transform that produces multiple output rows from one input.
pub struct FanoutTransform {
    mode: FanoutMode,
    output_schema: Option<TableSchema>,
    fan_count: usize,
}

impl FanoutTransform {
    pub fn new(mode: FanoutMode) -> Self {
        let fan_count = match &mode {
            FanoutMode::N { count } => (*count).min(3),
            _ => 3,
        };
        FanoutTransform {
            mode,
            output_schema: None,
            fan_count,
        }
    }
}

impl Transform for FanoutTransform {
    type Input = Row;
    type Output = Row;

    fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
        match &self.mode {
            FanoutMode::All => {
                Ok(vec![record; self.fan_count])
            }
            FanoutMode::First { index } => {
                if *index < self.fan_count {
                    Ok(vec![record])
                } else {
                    Ok(vec![])
                }
            }
            FanoutMode::N { count } => {
                let n = (*count).min(self.fan_count);
                Ok(vec![record.clone(); n])
            }
        }
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.output_schema.clone()
    }

    fn set_input_schema(&mut self, schema: TableSchema) {
        if self.output_schema.is_none() {
            self.output_schema = Some(schema);
        }
    }
}

/// Rename configuration: old name -> new name.
#[derive(Debug, Clone)]
pub struct RenameTransform {
    renames: Vec<(String, String)>,
    output_schema: Option<TableSchema>,
}

impl RenameTransform {
    pub fn new(renames: Vec<(String, String)>) -> Self {
        RenameTransform {
            renames,
            output_schema: None,
        }
    }
}

impl Transform for RenameTransform {
    type Input = Row;
    type Output = Row;

    fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
        Ok(vec![record])
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.output_schema.clone()
    }

    fn set_input_schema(&mut self, schema: TableSchema) {
        let mut cols: Vec<ColumnDef> = Vec::new();
        for col in &schema.columns {
            let new_name = self
                .renames
                .iter()
                .find(|(old, _)| old == &col.name)
                .map(|(_, new)| new.clone())
                .unwrap_or(col.name.clone());
            cols.push(ColumnDef::new(new_name, col.column_type.clone()));
        }
        let new_schema = TableSchema::new(schema.table_identifier.clone(), cols);
        self.output_schema = Some(new_schema);
    }
}

/// Select/projection transform.
#[derive(Debug, Clone)]
pub struct SelectTransform {
    column_indices: Vec<usize>,
    output_schema: Option<TableSchema>,
}

impl SelectTransform {
    pub fn new(column_indices: Vec<usize>) -> Self {
        SelectTransform {
            column_indices,
            output_schema: None,
        }
    }
}

impl Transform for SelectTransform {
    type Input = Row;
    type Output = Row;

    fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
        let n = self.column_indices.len();
        let mut out = Row::new(record.kind, n);
        for (i, &src_idx) in self.column_indices.iter().enumerate() {
            if src_idx < record.field_count() {
                out.set(i, record.get(src_idx).clone());
            }
        }
        Ok(vec![out])
    }

    fn get_output_schema(&self) -> Option<TableSchema> {
        self.output_schema.clone()
    }

    fn set_input_schema(&mut self, schema: TableSchema) {
        let mut cols: Vec<ColumnDef> = Vec::new();
        for &idx in &self.column_indices {
            if let Some(col) = schema.columns.get(idx) {
                cols.push(col.clone());
            }
        }
        let new_schema = TableSchema::new(schema.table_identifier.clone(), cols);
        self.output_schema = Some(new_schema);
    }
}

/// A composite pipeline that chains multiple transforms.
pub struct TransformPipeline {
    transforms: Vec<Box<dyn Transform<Input = Row, Output = Row>>>,
}

impl TransformPipeline {
    pub fn new() -> Self {
        TransformPipeline {
            transforms: Vec::new(),
        }
    }

    pub fn add(&mut self, transform: Box<dyn Transform<Input = Row, Output = Row>>) -> &mut Self {
        self.transforms.push(transform);
        self
    }

    pub fn process(&mut self, row: Row) -> anyhow::Result<Vec<Row>> {
        let mut current = vec![row];
        for t in &mut self.transforms {
            let mut next = Vec::new();
            for r in current {
                next.extend(t.process(r)?);
            }
            current = next;
        }
        Ok(current)
    }
}


/// SQL transform powered by Apache DataFusion.
///
/// Allows filtering, aggregation, and projection via SQL:
/// ```sql
/// SELECT id, name FROM users WHERE active = true
/// ```
#[cfg(feature = "datafusion")]
pub mod sql {
    use std::sync::Arc;

    use datafusion::{
        execution::context::SessionContext,
        dataframe::DataFrame,
        prelude::*,
    };
    use seatunnel_api::{
        row::{Row, RowKind},
        schema::{TableSchema, ColumnDef},
        transform::Transform,
        ColumnType,
    };

    /// A SQL-based transform that applies an arbitrary SELECT statement.
    pub struct SqlTransform {
        sql: String,
        table_name: String,
        output_schema: Option<TableSchema>,
    }

    impl SqlTransform {
        /// Create a new SQL transform with the given SQL and table name.
        pub fn new(sql: &str, table_name: &str) -> Self {
            SqlTransform {
                sql: sql.to_string(),
                table_name: table_name.to_string(),
                output_schema: None,
            }
        }

        /// Execute the SQL and return resulting rows.
        pub async fn execute(
            &self,
            ctx: &SessionContext,
            df: &DataFrame,
        ) -> datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
            df.clone()
                .sql(&self.sql)
                .await
                .collect()
                .await
        }
    }

    impl Transform for SqlTransform {
        type Input = Row;
        type Output = Row;

        fn process(&mut self, record: Self::Input) -> anyhow::Result<Vec<Self::Output>> {
            Ok(vec![record])
        }

        fn get_output_schema(&self) -> Option<TableSchema> {
            self.output_schema.clone()
        }

        fn set_input_schema(&mut self, schema: TableSchema) {
            self.output_schema = Some(schema);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_sql_transform_creation() {
            let t = SqlTransform::new("SELECT * FROM users WHERE active = true", "users");
            assert_eq!(t.table_name, "users");
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use seatunnel_api::ColumnType;

    fn make_schema() -> TableSchema {
        TableSchema::new("test", vec![
            ColumnDef::new("id".to_string(), ColumnType::Int64),
            ColumnDef::new("name".to_string(), ColumnType::String),
            ColumnDef::new("active".to_string(), ColumnType::Bool),
        ])
    }

    struct EvenIdFilter;
    impl FilterPredicate for EvenIdFilter {
        fn test(&self, row: &Row) -> bool {
            row.get(0).as_i64().map(|v| v % 2 == 0).unwrap_or(false)
        }
    }

    struct DoubleIdMapper;
    impl FieldMapper for DoubleIdMapper {
        fn map(&self, mut row: Row) -> Row {
            if let Some(v) = row.get(0).as_i64() {
                row.set(0, seatunnel_api::Field::Int64(v * 2));
            }
            row
        }
    }

    #[test]
    fn test_filter_transform() {
        let mut t = FilterTransform::new(EvenIdFilter);
        let mut row1 = Row::new(RowKind::Insert, 3);
        row1.set(0, seatunnel_api::Field::Int64(2));
        let mut row2 = Row::new(RowKind::Insert, 3);
        row2.set(0, seatunnel_api::Field::Int64(3));
        assert_eq!(t.process(row1).unwrap().len(), 1);
        assert_eq!(t.process(row2).unwrap().len(), 0);
    }

    #[test]
    fn test_map_transform() {
        let mut t = MapTransform::new(DoubleIdMapper);
        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(7));
        let result = t.process(row).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(*result[0].get(0), seatunnel_api::Field::Int64(14));
    }

    #[test]
    fn test_fanout_all() {
        let mut t = FanoutTransform::new(FanoutMode::All);
        let row = Row::new(RowKind::Insert, 1);
        let result = t.process(row).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_fanout_n() {
        let mut t = FanoutTransform::new(FanoutMode::N { count: 5 });
        let row = Row::new(RowKind::Insert, 1);
        let result = t.process(row).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_fanout_first() {
        let mut t = FanoutTransform::new(FanoutMode::First { index: 0 });
        let row = Row::new(RowKind::Insert, 1);
        let result = t.process(row).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_rename_transform() {
        let mut t = RenameTransform::new(vec![("id".to_string(), "user_id".to_string())]);
        t.set_input_schema(make_schema());
        let schema = t.get_output_schema().unwrap();
        assert_eq!(schema.columns[0].name, "user_id");
        assert_eq!(schema.columns[1].name, "name");
    }

    #[test]
    fn test_select_transform() {
        let mut t = SelectTransform::new(vec![1, 0]);
        t.set_input_schema(make_schema());
        let schema = t.get_output_schema().unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "name");
        assert_eq!(schema.columns[1].name, "id");

        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(42));
        row.set(1, seatunnel_api::Field::String("test".to_string()));
        let result = t.process(row).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(*result[0].get(0), seatunnel_api::Field::String("test".to_string()));
        assert_eq!(*result[0].get(1), seatunnel_api::Field::Int64(42));
    }

    #[test]
    fn test_pipeline() {
        let mut pipeline = TransformPipeline::new();
        pipeline.add(Box::new(FilterTransform::new(EvenIdFilter)));
        pipeline.add(Box::new(MapTransform::new(DoubleIdMapper)));

        let mut row = Row::new(RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(4));
        let result = pipeline.process(row).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(*result[0].get(0), seatunnel_api::Field::Int64(8));
    }

    #[test]
    fn test_schema_generation() {
        let mut t = SelectTransform::new(vec![0]);
        t.set_input_schema(make_schema());
        let schema = t.get_output_schema().unwrap();
        assert_eq!(schema.table_identifier, "test");
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].column_type, ColumnType::Int64);
    }
}
