use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let kind = match obj.get("op").and_then(|v| v.as_str()) {
        Some("c") | Some("r") => seatunnel_api::RowKind::Insert,
        Some("u") => seatunnel_api::RowKind::UpdateAfter,
        Some("d") => seatunnel_api::RowKind::Delete,
        _ => seatunnel_api::RowKind::Insert,
    };
    let empty_map: serde_json::Map<String, Value> = serde_json::Map::<String, Value>::default();
    let data = if kind == seatunnel_api::RowKind::Delete {
        match obj.get("before").and_then(|v| v.as_object()) { Some(m) => m, None => &empty_map }
    } else {
        match obj.get("after").and_then(|v| v.as_object()) { Some(m) => m, None => &empty_map }
    };
    let mut row = Row::new(kind, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        row.set(i, json_to_field(data.get(&col.name))?);
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    let ts = chrono::Utc::now().timestamp_millis() as i64;
    let (op, has_before, has_after) = match row.kind {
        seatunnel_api::RowKind::Insert => ("c", false, true),
        seatunnel_api::RowKind::UpdateAfter => ("u", true, true),
        seatunnel_api::RowKind::Delete => ("d", true, false),
        _ => ("c", false, true),
    };
    if has_after { obj.insert("after".to_string(), row_to_json_map(schema, row)?); }
    if has_before { obj.insert("before".to_string(), row_to_json_map(schema, row)?); }
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
        if i < row.field_count() { map.insert(col.name.clone(), field_to_json(row.get(i))?); }
    }
    Ok(Value::Object(map))
}

fn json_to_field(value: Option<&Value>) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    let value = match value { Some(v) => v, None => return Ok(seatunnel_api::Field::Null) };
    match value {
        Value::Bool(b) => Ok(seatunnel_api::Field::Bool(*b)),
        Value::Number(n) => { if let Some(i) = n.as_i64() { Ok(seatunnel_api::Field::Int64(i)) } else if let Some(u) = n.as_u64() { Ok(seatunnel_api::Field::UInt64(u)) } else if let Some(f) = n.as_f64() { Ok(seatunnel_api::Field::Float64(f)) } else { Ok(seatunnel_api::Field::Null) } }
        Value::String(s) => Ok(seatunnel_api::Field::String(s.clone())),
        Value::Array(arr) => { let fields: Vec<seatunnel_api::Field> = arr.iter().map(|v| json_to_field(Some(v))).collect::<Result<_,_>>()?; Ok(seatunnel_api::Field::Array(fields)) }
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
        TableSchema::new("t", vec![
            ColumnDef::new("id".to_string(), ColumnType::Int64),
            ColumnDef::new("name".to_string(), ColumnType::String),
        ])
    }
    #[test]
    fn test_debezium_insert() {
        let row = deserialize(b"{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\"},\"op\":\"c\",\"source\":{},\"ts_ms\":1000}", &make_schema()).unwrap();
        assert_eq!(row.kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(1));
    }
    #[test]
    fn test_debezium_delete() {
        let row = deserialize(b"{\"before\":{\"id\":2},\"after\":null,\"op\":\"d\"}", &make_schema()).unwrap();
        assert_eq!(row.kind, seatunnel_api::RowKind::Delete);
    }
}
