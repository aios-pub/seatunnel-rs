use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        row.set(i, json_to_field(obj.get(&col.name))?);
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut map = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() {
            map.insert(col.name.clone(), field_to_json(row.get(i))?);
        }
    }
    serde_json::to_vec(&Value::Object(map)).map_err(|e| format!("JSON error: {}", e).into())
}

fn json_to_field(value: Option<&Value>) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    let value = match value { Some(v) => v, None => return Ok(seatunnel_api::Field::Null) };
    match value {
        Value::Bool(b) => Ok(seatunnel_api::Field::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() { Ok(seatunnel_api::Field::Int64(i)) }
            else if let Some(u) = n.as_u64() { Ok(seatunnel_api::Field::UInt64(u)) }
            else if let Some(f) = n.as_f64() { Ok(seatunnel_api::Field::Float64(f)) }
            else { Ok(seatunnel_api::Field::Null) }
        }
        Value::String(s) => Ok(seatunnel_api::Field::String(s.clone())),
        Value::Array(arr) => {
            let fields: Vec<seatunnel_api::Field> = arr.iter().map(|v| json_to_field(Some(v))).collect::<Result<_,_>>()?;
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
        seatunnel_api::Field::Int32(v) => Ok(Value::Number((*v as i64).into())),
        seatunnel_api::Field::Int64(v) => Ok(Value::Number((*v).into())),
        seatunnel_api::Field::UInt64(v) => Ok(Value::Number((*v).into())),
        seatunnel_api::Field::Float64(v) => Ok(serde_json::Number::from_f64(*v).map(Value::Number).unwrap_or(Value::Null)),
        seatunnel_api::Field::String(s) => Ok(Value::String(s.clone())),
        seatunnel_api::Field::Bytes(b) => Ok(Value::String(hex::encode(b))),
        seatunnel_api::Field::Json(v) => Ok(v.clone()),
        seatunnel_api::Field::Date(d) => Ok(Value::String(d.to_string())),
        seatunnel_api::Field::Time(t) => Ok(Value::String(t.to_string())),
        seatunnel_api::Field::DateTime(dt) => Ok(Value::String(dt.to_string())),
        seatunnel_api::Field::TimestampTz(ts) => Ok(Value::String(ts.to_rfc3339())),
        seatunnel_api::Field::Array(arr) => {
            let vals: Vec<Value> = arr.iter().map(field_to_json).collect::<Result<_,_>>()?;
            Ok(Value::Array(vals))
        }
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
            ColumnDef::new("active".to_string(), ColumnType::Bool),
        ])
    }
    #[test]
    fn test_json_deserialize() {
        let schema = make_schema();
        let row = deserialize(b"{\"id\":42,\"name\":\"hello\",\"active\":true}", &schema).unwrap();
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(42));
        assert_eq!(*row.get(1), seatunnel_api::Field::String("hello".to_string()));
        assert_eq!(*row.get(2), seatunnel_api::Field::Bool(true));
    }
    #[test]
    fn test_json_roundtrip() {
        let schema = make_schema();
        let mut row = Row::new(seatunnel_api::RowKind::Insert, 3);
        row.set(0, seatunnel_api::Field::Int64(99));
        row.set(1, seatunnel_api::Field::String("world".to_string()));
        row.set(2, seatunnel_api::Field::Bool(false));
        let bytes = serialize(&schema, &row).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["id"].as_i64(), Some(99));
        assert_eq!(value["name"].as_str(), Some("world"));
    }
}
