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

//! Performance benchmarks for SeaTunnel Rust.
//!
//! Benchmarks cover:
//! - Row allocation and field population
//! - JSON serialization/deserialization
//! - Format-specific encoding/decoding
//! - Canal/Debezium JSON parsing

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use seatunnel_api::{
    row::{Row, RowKind},
    schema::{TableSchema, ColumnDef},
    ColumnType,
};
use seatunnel_formats::MessageFormat;

fn make_schema() -> TableSchema {
    TableSchema::new("users", vec![
        ColumnDef::new("id".to_string(), ColumnType::Int64),
        ColumnDef::new("name".to_string(), ColumnType::String),
        ColumnDef::new("email".to_string(), ColumnType::String),
        ColumnDef::new("score".to_string(), ColumnType::Float64),
        ColumnDef::new("active".to_string(), ColumnType::Bool),
    ])
}

fn make_row(rng: &mut SmallRng) -> Row {
    let mut row = Row::new(RowKind::Insert, 5);
    row.set(0, seatunnel_api::Field::Int64(rng.gen_range(0..1_000_000)));
    row.set(1, seatunnel_api::Field::String(format!("user_{}", rng.gen_range(0..10_000))));
    row.set(
        2,
        seatunnel_api::Field::String(format!(
            "{}@example.com",
            rng.gen_range(0..100_000)
        )),
    );
    row.set(3, seatunnel_api::Field::Float64(rng.gen_range(0.0..100.0)));
    row.set(4, seatunnel_api::Field::Bool(rng.gen()));
    row
}

fn bench_row_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_allocation");
    group.bench_function("new_row_insert", |b| {
        b.iter(|| {
            let row = Row::new(RowKind::Insert, 5);
            black_box(row)
        })
    });
    group.bench_function("new_row_populated", |b| {
        let mut rng = SmallRng::seed_from_u64(42);
        b.iter(|| black_box(make_row(&mut rng)))
    });
    group.finish();
}

fn bench_json_serialization(c: &mut Criterion) {
    let schema = make_schema();
    let mut rng = SmallRng::seed_from_u64(42);
    let row = make_row(&mut rng);
    let mut group = c.benchmark_group("json_serialization");
    group.bench_function("json_serialize", |b| {
        b.iter(|| {
            black_box(seatunnel_formats::serialize(
                MessageFormat::Json,
                &schema,
                &row,
            ))
        })
    });
    group.finish();
}

fn bench_json_deserialization(c: &mut Criterion) {
    let schema = make_schema();
    let mut rng = SmallRng::seed_from_u64(42);
    let row = make_row(&mut rng);
    let bytes = seatunnel_formats::serialize(MessageFormat::Json, &schema, &row).unwrap();
    let mut group = c.benchmark_group("json_deserialization");
    group.bench_function("json_deserialize", |b| {
        b.iter(|| black_box(seatunnel_formats::deserialize(MessageFormat::Json, &bytes, &schema)))
    });
    group.finish();
}

fn bench_debezium_serialization(c: &mut Criterion) {
    let schema = make_schema();
    let mut rng = SmallRng::seed_from_u64(42);
    let row = make_row(&mut rng);
    let mut group = c.benchmark_group("debezium_json");
    group.bench_function("debezium_serialize", |b| {
        b.iter(|| {
            black_box(seatunnel_formats::serialize(
                MessageFormat::DebeziumJson,
                &schema,
                &row,
            ))
        })
    });
    let bytes = seatunnel_formats::serialize(MessageFormat::DebeziumJson, &schema, &row).unwrap();
    group.bench_function("debezium_deserialize", |b| {
        b.iter(|| {
            black_box(seatunnel_formats::deserialize(
                MessageFormat::DebeziumJson,
                &bytes,
                &schema,
            ))
        })
    });
    group.finish();
}

fn bench_text_serialization(c: &mut Criterion) {
    let schema = make_schema();
    let mut rng = SmallRng::seed_from_u64(42);
    let row = make_row(&mut rng);
    let mut group = c.benchmark_group("text_serialization");
    group.bench_function("text_serialize", |b| {
        b.iter(|| {
            black_box(seatunnel_formats::serialize(MessageFormat::Text, &schema, &row))
        })
    });
    let bytes = seatunnel_formats::serialize(MessageFormat::Text, &schema, &row).unwrap();
    group.bench_function("text_deserialize", |b| {
        b.iter(|| {
            black_box(seatunnel_formats::deserialize(MessageFormat::Text, &bytes, &schema))
        })
    });
    group.finish();
}

fn bench_schema_lookup(c: &mut Criterion) {
    let schema = make_schema();
    let mut group = c.benchmark_group("schema_operations");
    group.bench_function("column_index_lookup", |b| {
        b.iter(|| black_box(schema.column_index("email")))
    });
    group.bench_function("field_names", |b| {
        b.iter(|| black_box(schema.field_names()))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_row_allocation,
    bench_json_serialization,
    bench_json_deserialization,
    bench_debezium_serialization,
    bench_text_serialization,
    bench_schema_lookup
);
criterion_main!(benches);

#[cfg(test)]
mod tests {
    #[test]
    fn test_benchmark_schema() {
        use super::*;
        let schema = make_schema();
        assert_eq!(schema.column_count(), 5);
        assert_eq!(schema.column_index("email"), Some(2));
    }

    #[test]
    fn test_benchmark_row() {
        use super::*;
        let mut rng = SmallRng::seed_from_u64(42);
        let row = make_row(&mut rng);
        assert_eq!(row.field_count(), 5);
        assert!(row.get(0).as_i64().is_some());
        assert!(row.get(4).as_bool().is_some());
    }
}
