use seatunnel_api::{Row, TableSchema};
use serde_json::Value;
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        row.set(i, json_to_field(obj.get(&col.name))?);
    }
    Ok(vec![row])
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut map = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() {
            map.insert(col.name.clone(), field_to_json(row.get(i))?);
        }
    }
    serde_json::to_vec(&Value::Object(map)).map_err(|e| format!("Native error: {}", e).into())
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
            } else {
                Ok(seatunnel_api::Field::String(n.to_string()))
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
        seatunnel_api::Field::Int32(v) => Ok(Value::Number((*v as i64).into())),
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
            "kafka",
            vec![
                ColumnDef::new("topic".to_string(), ColumnType::String),
                ColumnDef::new("offset".to_string(), ColumnType::Int64),
            ],
        )
    }
    #[test]
    fn test_native_roundtrip() {
        let schema = make_schema();
        let rows = deserialize(b"{\"topic\":\"t1\",\"offset\":12345}", &schema).unwrap();
        let row = &rows[0];
        assert_eq!(*row.get(0), seatunnel_api::Field::String("t1".to_string()));
        assert_eq!(*row.get(1), seatunnel_api::Field::Int64(12345));
    }
}
