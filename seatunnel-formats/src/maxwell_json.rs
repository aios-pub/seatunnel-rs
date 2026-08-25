use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let kind = match obj.get("type").and_then(|v| v.as_str()) {
        Some("insert") | Some("bootstrap-insert") => seatunnel_api::RowKind::Insert,
        Some("delete") => seatunnel_api::RowKind::Delete,
        Some("update") => seatunnel_api::RowKind::UpdateAfter,
        _ => seatunnel_api::RowKind::Insert,
    };
    let empty_map: serde_json::Map<String, Value> = serde_json::Map::<String, Value>::default();
    let data = match obj.get("data").and_then(|v| v.as_object()) { Some(m) => m, None => &empty_map };
    let mut row = Row::new(kind, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        row.set(i, json_to_field(data.get(&col.name))?);
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    obj.insert("database".to_string(), Value::String("default".to_string()));
    obj.insert("table".to_string(), Value::String(schema.table_identifier.clone()));
    obj.insert("type".to_string(), Value::String(match row.kind { seatunnel_api::RowKind::Delete => "delete".to_string(), _ => "insert".to_string() }));
    obj.insert("xoffset".to_string(), Value::Number(0.into()));
    obj.insert("commit".to_string(), Value::Bool(true));
    let mut data = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() { data.insert(col.name.clone(), field_to_json(row.get(i))?); }
    }
    obj.insert("data".to_string(), Value::Object(data));
    serde_json::to_vec(&Value::Object(obj)).map_err(|e| format!("Maxwell error: {}", e).into())
}

fn json_to_field(value: Option<&Value>) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    let value = match value { Some(v) => v, None => return Ok(seatunnel_api::Field::Null) };
    match value {
        Value::Bool(b) => Ok(seatunnel_api::Field::Bool(*b)),
        Value::Number(n) => { if let Some(i) = n.as_i64() { Ok(seatunnel_api::Field::Int64(i)) } else if let Some(u) = n.as_u64() { Ok(seatunnel_api::Field::UInt64(u)) } else if let Some(f) = n.as_f64() { Ok(seatunnel_api::Field::Float64(f)) } else { Ok(seatunnel_api::Field::Null) } }
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
        TableSchema::new("t", vec![
            ColumnDef::new("id".to_string(), ColumnType::Int64),
            ColumnDef::new("name".to_string(), ColumnType::String),
        ])
    }
    #[test]
    fn test_maxwell_insert() {
        let row = deserialize(b"{\"type\":\"insert\",\"data\":{\"id\":1,\"name\":\"hello\"}}", &make_schema()).unwrap();
        assert_eq!(row.kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(1));
    }
}
