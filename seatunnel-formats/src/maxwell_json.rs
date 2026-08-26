use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let kind_str = obj.get("type").and_then(|v| v.as_str()).unwrap_or("insert");
    let data = obj.get("data").and_then(|v| v.as_object()).cloned().unwrap_or_default();
    let old = obj.get("old").and_then(|v| v.as_object()).cloned().unwrap_or_default();
    let mut rows = Vec::new();
    match kind_str {
        "insert" | "bootstrap-insert" => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() { row.set(i, json_to_field(data.get(&col.name))?); }
            rows.push(row);
        }
        "update" => {
            let mut br = Row::new(seatunnel_api::RowKind::UpdateBefore, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() { br.set(i, json_to_field(old.get(&col.name))?); }
            rows.push(br);
            let mut ar = Row::new(seatunnel_api::RowKind::UpdateAfter, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() { ar.set(i, json_to_field(data.get(&col.name))?); }
            rows.push(ar);
        }
        "delete" => {
            let mut row = Row::new(seatunnel_api::RowKind::Delete, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() { row.set(i, json_to_field(data.get(&col.name))?); }
            rows.push(row);
        }
        _ => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() { row.set(i, json_to_field(data.get(&col.name))?); }
            rows.push(row);
        }
    }
    Ok(rows)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    obj.insert("database".to_string(), Value::String("default".to_string()));
    obj.insert("table".to_string(), Value::String(schema.table_identifier.clone()));
    let ts = match row.kind {
        seatunnel_api::RowKind::Delete => "delete",
        seatunnel_api::RowKind::UpdateBefore => "update",
        seatunnel_api::RowKind::UpdateAfter => "update",
        _ => "insert",
    };
    obj.insert("type".to_string(), Value::String(ts.to_string()));
    obj.insert("xoffset".to_string(), Value::Number(0.into()));
    obj.insert("commit".to_string(), Value::Bool(true));
    let mut data = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() { data.insert(col.name.clone(), field_to_json(row.get(i))?); }
    }
    // For UpdateBefore, clone data for both data and old fields
    let is_update_before = row.kind == seatunnel_api::RowKind::UpdateBefore;
    if is_update_before {
        obj.insert("old".to_string(), serde_json::to_value(&data).unwrap());
    }
    obj.insert("data".to_string(), serde_json::to_value(&data).unwrap());
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
        TableSchema::new("users", vec![
            ColumnDef::new("id".to_string(), ColumnType::Int64),
            ColumnDef::new("name".to_string(), ColumnType::String),
        ])
    }
    #[test]
    fn test_maxwell_insert() {
        let bytes = b"{\"database\":\"test\",\"table\":\"users\",\"type\":\"insert\",\"ts\":1000,\"data\":{\"id\":1,\"name\":\"alice\"}}";
        let rows = deserialize(bytes, &make_schema()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*rows[0].get(0), seatunnel_api::Field::Int64(1));
    }
    #[test]
    fn test_maxwell_update() {
        let bytes = b"{\"database\":\"test\",\"table\":\"users\",\"type\":\"update\",\"ts\":1000,\"data\":{\"id\":1,\"name\":\"new\"},\"old\":{\"id\":1,\"name\":\"old\"}}";
        let rows = deserialize(bytes, &make_schema()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::UpdateBefore);
        assert_eq!(rows[1].kind, seatunnel_api::RowKind::UpdateAfter);
    }
    #[test]
    fn test_maxwell_delete() {
        let bytes = b"{\"database\":\"test\",\"table\":\"users\",\"type\":\"delete\",\"ts\":1000,\"data\":{\"id\":2}}";
        let rows = deserialize(bytes, &make_schema()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::Delete);
    }
}
