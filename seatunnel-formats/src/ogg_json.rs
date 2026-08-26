use seatunnel_api::{Row, TableSchema};
use serde_json::Value;
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let op_str = obj
        .get("OP_TYPE")
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
        .unwrap_or_default();
    let empty_arr: Vec<Value> = Vec::new();
    let after = match obj.get("AFTER_VALUE").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => &empty_arr,
    };
    let before = match obj.get("BEFORE_VALUE").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => &empty_arr,
    };
    let cols = match obj.get("COLUMN_NAME").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => &empty_arr,
    };

    let mut rows = Vec::new();

    match op_str.as_str() {
        "INSERT" => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                let idx = (0..cols.len()).find(|&j| {
                    cols[j]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(&col.name))
                });
                let field = match idx {
                    Some(j) if j < after.len() => json_to_field(Some(&after[j]))?,
                    _ => json_to_field(None)?,
                };
                row.set(i, field);
            }
            rows.push(row);
        }
        "DELETE" => {
            let mut row = Row::new(seatunnel_api::RowKind::Delete, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                let idx = (0..cols.len()).find(|&j| {
                    cols[j]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(&col.name))
                });
                let field = match idx {
                    Some(j) if j < before.len() => json_to_field(Some(&before[j]))?,
                    _ => json_to_field(None)?,
                };
                row.set(i, field);
            }
            rows.push(row);
        }
        "UPDATE" => {
            // Emit UpdateBefore from BEFORE_VALUE
            let mut before_row =
                Row::new(seatunnel_api::RowKind::UpdateBefore, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                let idx = (0..cols.len()).find(|&j| {
                    cols[j]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(&col.name))
                });
                let field = match idx {
                    Some(j) if j < before.len() => json_to_field(Some(&before[j]))?,
                    _ => json_to_field(None)?,
                };
                before_row.set(i, field);
            }
            rows.push(before_row);

            // Emit UpdateAfter from AFTER_VALUE
            let mut after_row =
                Row::new(seatunnel_api::RowKind::UpdateAfter, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                let idx = (0..cols.len()).find(|&j| {
                    cols[j]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(&col.name))
                });
                let field = match idx {
                    Some(j) if j < after.len() => json_to_field(Some(&after[j]))?,
                    _ => json_to_field(None)?,
                };
                after_row.set(i, field);
            }
            rows.push(after_row);
        }
        _ => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                let idx = (0..cols.len()).find(|&j| {
                    cols[j]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(&col.name))
                });
                let field = match idx {
                    Some(j) if j < after.len() => json_to_field(Some(&after[j]))?,
                    _ => json_to_field(None)?,
                };
                row.set(i, field);
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    let op_type = match row.kind {
        seatunnel_api::RowKind::Delete => "DELETE".to_string(),
        seatunnel_api::RowKind::UpdateBefore | seatunnel_api::RowKind::UpdateAfter => {
            "MODIFY".to_string()
        }
        _ => "INSERT".to_string(),
    };
    obj.insert("OP_TYPE".to_string(), Value::String(op_type));
    let col_names: Vec<Value> = schema
        .columns
        .iter()
        .map(|c| Value::String(c.name.clone()))
        .collect();
    let after_vals: Vec<Value> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i < row.field_count() {
                field_to_json(row.get(i)).unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        })
        .collect();
    obj.insert("COLUMN_NAME".to_string(), Value::Array(col_names));
    obj.insert("AFTER_VALUE".to_string(), Value::Array(after_vals));
    serde_json::to_vec(&Value::Object(obj)).map_err(|e| format!("OGG error: {}", e).into())
}

fn json_to_field(value: Option<&Value>) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    let value = match value {
        Some(v) => v,
        None => return Ok(seatunnel_api::Field::Null),
    };
    match value {
        Value::Bool(b) => Ok(seatunnel_api::Field::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(seatunnel_api::Field::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(seatunnel_api::Field::UInt64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(seatunnel_api::Field::Float64(f))
            } else {
                Ok(seatunnel_api::Field::Null)
            }
        }
        Value::String(s) => Ok(seatunnel_api::Field::String(s.clone())),
        Value::Null => Ok(seatunnel_api::Field::Null),
        _ => Ok(seatunnel_api::Field::Null),
    }
}

fn field_to_json(field: &seatunnel_api::Field) -> Result<Value, Box<dyn Error>> {
    match field {
        seatunnel_api::Field::Null => Ok(Value::Null),
        seatunnel_api::Field::Bool(b) => Ok(Value::Bool(*b)),
        seatunnel_api::Field::Int64(v) => Ok(Value::Number((*v).into())),
        seatunnel_api::Field::String(s) => Ok(Value::String(s.clone())),
        _ => Ok(Value::String(format!("{}", field))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seatunnel_api::{ColumnDef, ColumnType};
    fn make_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::new("id".to_string(), ColumnType::Int64),
                ColumnDef::new("name".to_string(), ColumnType::String),
            ],
        )
    }
    #[test]
    fn test_ogg_insert() {
        let rows = deserialize(b"{\"OP_TYPE\":\"INSERT\",\"COLUMN_NAME\":[\"id\",\"name\"],\"AFTER_VALUE\":[1,\"alice\"]}", &make_schema())
.unwrap();
        let row = &rows[0];
        assert_eq!(row.kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(1));
    }

    #[test]
    fn test_ogg_update() {
        let rows = deserialize(b"{\"OP_TYPE\":\"UPDATE\",\"COLUMN_NAME\":[\"id\",\"name\"],\"BEFORE_VALUE\":[1,\"old\"],\"AFTER_VALUE\":[1,\"new\"]}", &make_schema())
.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::UpdateBefore);
        assert_eq!(rows[1].kind, seatunnel_api::RowKind::UpdateAfter);
        assert_eq!(
            *rows[0].get(1),
            seatunnel_api::Field::String("old".to_string())
        );
        assert_eq!(
            *rows[1].get(1),
            seatunnel_api::Field::String("new".to_string())
        );
    }

    #[test]
    fn test_ogg_delete() {
        let rows = deserialize(b"{\"OP_TYPE\":\"DELETE\",\"COLUMN_NAME\":[\"id\",\"name\"],\"BEFORE_VALUE\":[2,\"bob\"]}", &make_schema())
.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::Delete);
        assert_eq!(*rows[0].get(0), seatunnel_api::Field::Int64(2));
    }

    #[test]
    fn test_ogg_lowercase_op() {
        // OP_TYPE value is lowercase but keys are uppercase (OGG standard)
        let rows = deserialize(b"{\"OP_TYPE\":\"update\",\"COLUMN_NAME\":[\"id\"],\"BEFORE_VALUE\":[1],\"AFTER_VALUE\":[2]}", &make_schema())
.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::UpdateBefore);
        assert_eq!(rows[1].kind, seatunnel_api::RowKind::UpdateAfter);
    }
}
