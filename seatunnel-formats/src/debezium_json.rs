use seatunnel_api::{Row, TableSchema};
use serde_json::Value;
use std::error::Error;

/// Deserialize a Debezium JSON event.
/// For UPDATE events, returns both UpdateBefore and UpdateAfter rows.
pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let op = obj.get("op").and_then(|v| v.as_str()).unwrap_or("c");

    let before_map: serde_json::Map<String, Value> = obj
        .get("before")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let after_map: serde_json::Map<String, Value> = obj
        .get("after")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut rows = Vec::new();

    match op {
        "c" | "r" => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                row.set(i, map_to_field(&before_map, &after_map, &col.name, true)?);
            }
            rows.push(row);
        }
        "u" => {
            // UPDATE: emit UpdateBefore (from "before") then UpdateAfter (from "after")
            let mut before_row =
                Row::new(seatunnel_api::RowKind::UpdateBefore, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                before_row.set(i, map_to_field(&before_map, &after_map, &col.name, false)?);
            }
            rows.push(before_row);

            let mut after_row =
                Row::new(seatunnel_api::RowKind::UpdateAfter, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                after_row.set(i, map_to_field(&before_map, &after_map, &col.name, true)?);
            }
            rows.push(after_row);
        }
        "d" => {
            let mut row = Row::new(seatunnel_api::RowKind::Delete, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                row.set(i, map_to_field(&before_map, &after_map, &col.name, false)?);
            }
            rows.push(row);
        }
        _ => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                row.set(i, map_to_field(&before_map, &after_map, &col.name, true)?);
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

fn map_to_field(
    before: &serde_json::Map<String, Value>,
    after: &serde_json::Map<String, Value>,
    name: &str,
    prefer_after: bool,
) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    // When prefer_after is true, try after first (for UpdateAfter/Insert), else try before first (for UpdateBefore/Delete)
    let first = if prefer_after { after } else { before };
    let second = if prefer_after { before } else { after };
    if let Some(v) = first.get(name) {
        json_to_field(Some(v))
    } else if let Some(v) = second.get(name) {
        json_to_field(Some(v))
    } else {
        Ok(seatunnel_api::Field::Null)
    }
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    let ts = chrono::Utc::now().timestamp_millis();
    let (op, has_before, has_after) = match row.kind {
        seatunnel_api::RowKind::Insert => ("c", false, true),
        seatunnel_api::RowKind::UpdateBefore => ("u", true, false),
        seatunnel_api::RowKind::UpdateAfter => ("u", true, true),
        seatunnel_api::RowKind::Delete => ("d", true, false),
    };
    if has_after {
        obj.insert("after".to_string(), row_to_json_map(schema, row)?);
    }
    if has_before {
        obj.insert("before".to_string(), row_to_json_map(schema, row)?);
    }
    obj.insert("op".to_string(), Value::String(op.to_string()));
    let mut source = serde_json::Map::<String, Value>::default();
    source.insert("ts_ms".to_string(), Value::Number(ts.into()));
    obj.insert("source".to_string(), Value::Object(source));
    obj.insert("ts_ms".to_string(), Value::Number(ts.into()));
    serde_json::to_vec(&Value::Object(obj)).map_err(|e| format!("Debezium error: {}", e).into())
}

fn row_to_json_map(schema: &TableSchema, row: &Row) -> Result<Value, Box<dyn Error>> {
    let mut map = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() {
            map.insert(col.name.clone(), field_to_json(row.get(i))?);
        }
    }
    Ok(Value::Object(map))
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
        Value::Array(arr) => {
            let fields: Vec<seatunnel_api::Field> = arr
                .iter()
                .map(|v| json_to_field(Some(v)))
                .collect::<Result<_, _>>()?;
            Ok(seatunnel_api::Field::Array(fields))
        }
        Value::Object(_) => Ok(seatunnel_api::Field::Json(value.clone())),
        Value::Null => Ok(seatunnel_api::Field::Null),
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
            "t",
            vec![
                ColumnDef::new("id".to_string(), ColumnType::Int64),
                ColumnDef::new("name".to_string(), ColumnType::String),
            ],
        )
    }
    #[test]
    fn test_debezium_insert() {
        let rows = deserialize(b"{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\"},\"op\":\"c\",\"source\":{},\"ts_ms\":1000}", &make_schema())
.unwrap();
        let _row = &rows[0];
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(1));
    }
    #[test]
    fn test_debezium_update() {
        let rows = deserialize(b"{\"before\":{\"id\":1,\"name\":\"old\"},\"after\":{\"id\":1,\"name\":\"new\"},\"op\":\"u\"}", &make_schema())
.unwrap();
        let _row = &rows[0];
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
    fn test_debezium_delete() {
        let rows = deserialize(
            b"{\"before\":{\"id\":2},\"after\":null,\"op\":\"d\"}",
            &make_schema(),
        )
        .unwrap();
        let _row = &rows[0];
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, seatunnel_api::RowKind::Delete);
    }
}
