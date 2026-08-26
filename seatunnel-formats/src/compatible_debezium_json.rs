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
    serde_json::to_vec(&Value::Object(map))
        .map_err(|e| format!("Compat Debezium error: {}", e).into())
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
    fn test_roundtrip() {
        let rows = deserialize(b"{\"id\":42,\"name\":\"hello\"}", &make_schema()).unwrap();
        let _row = &rows[0];
        let row = &rows[0];
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(42));
        assert_eq!(
            *row.get(1),
            seatunnel_api::Field::String("hello".to_string())
        );
    }
}
