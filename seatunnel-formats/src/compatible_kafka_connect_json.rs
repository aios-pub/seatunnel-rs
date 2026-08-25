use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let empty_map: serde_json::Map<String, Value> = serde_json::Map::<String, Value>::default();
    let payload = match obj.get("payload").and_then(|v| v.as_object()) { Some(m) => m, None => &empty_map };
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        row.set(i, json_to_field(payload.get(&col.name))?);
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut payload = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() { payload.insert(col.name.clone(), field_to_json(row.get(i))?); }
    }
    let mut fields = Vec::new();
    for col in &schema.columns {
        let mut f = serde_json::Map::<String, Value>::default();
        f.insert("type".to_string(), Value::String(field_type_str(&col.column_type)));
        f.insert("field".to_string(), Value::String(col.name.clone()));
        fields.push(Value::Object(f));
    }
    let mut schema_obj = serde_json::Map::<String, Value>::default();
    schema_obj.insert("type".to_string(), Value::String("struct".to_string()));
    schema_obj.insert("fields".to_string(), Value::Array(fields));
    let mut obj = serde_json::Map::<String, Value>::default();
    obj.insert("schema".to_string(), Value::Object(schema_obj));
    obj.insert("payload".to_string(), Value::Object(payload));
    serde_json::to_vec(&Value::Object(obj)).map_err(|e| format!("Kafka Connect error: {}", e).into())
}

fn field_type_str(col_type: &seatunnel_api::ColumnType) -> String {
    match col_type {
        seatunnel_api::ColumnType::Bool => "boolean".to_string(),
        seatunnel_api::ColumnType::Int32 => "int32".to_string(),
        seatunnel_api::ColumnType::Int64 => "int64".to_string(),
        seatunnel_api::ColumnType::Float64 => "double".to_string(),
        seatunnel_api::ColumnType::Bytes => "bytes".to_string(),
        _ => "string".to_string(),
    }
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
    fn test_deserialize() {
        let row = deserialize(b"{\"schema\":{},\"payload\":{\"id\":42,\"name\":\"hello\"}}", &make_schema()).unwrap();
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(42));
    }
    #[test]
    fn test_null_payload() {
        let row = deserialize(b"{\"schema\":{},\"payload\":null}", &make_schema()).unwrap();
        assert!(row.get(0).is_null());
    }
}
