use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let kind_str = obj.get("_type").and_then(|v| v.as_str()).unwrap_or("INSERT");
    
    let empty_map: serde_json::Map<String, Value> = serde_json::Map::<String, Value>::default();
    let data = obj.get("data").and_then(|v| v.as_array()).and_then(|a| a.first()).and_then(|v| v.as_object())
        .cloned().unwrap_or(empty_map.clone());
    let old = obj.get("old").and_then(|v| v.as_array()).and_then(|a| a.first()).and_then(|v| v.as_object())
        .cloned().unwrap_or(empty_map);
    
    let mut rows = Vec::new();
    match kind_str {
        "INSERT" => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                row.set(i, map_val(&data, &col.name)?);
            }
            rows.push(row);
        }
        "UPDATE" => {
            let mut before = Row::new(seatunnel_api::RowKind::UpdateBefore, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                before.set(i, map_val(&old, &col.name)?);
            }
            rows.push(before);
            let mut after = Row::new(seatunnel_api::RowKind::UpdateAfter, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                after.set(i, map_val(&data, &col.name)?);
            }
            rows.push(after);
        }
        "DELETE" => {
            let mut row = Row::new(seatunnel_api::RowKind::Delete, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                row.set(i, map_val(&data, &col.name)?);
            }
            rows.push(row);
        }
        _ => {
            let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
            for (i, col) in schema.columns.iter().enumerate() {
                row.set(i, map_val(&data, &col.name)?);
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn map_val(map: &serde_json::Map<String, Value>, name: &str) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    json_to_field(map.get(name))
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    let ts = chrono::Utc::now().timestamp_millis() as i64;
    let type_str = match row.kind {
        seatunnel_api::RowKind::Delete => "DELETE",
        seatunnel_api::RowKind::UpdateBefore | seatunnel_api::RowKind::UpdateAfter => "UPDATE",
        _ => "INSERT",
    };
    obj.insert("_type".to_string(), Value::String(type_str.to_string()));
    obj.insert("_ts".to_string(), Value::Number(ts.into()));
    let mut data_obj = serde_json::Map::<String, Value>::default();
    for (i, col) in schema.columns.iter().enumerate() {
        if i < row.field_count() { data_obj.insert(col.name.clone(), field_to_json(row.get(i))?); }
    }
    obj.insert("data".to_string(), Value::Array(vec![Value::Object(data_obj)]));
    serde_json::to_vec(&Value::Object(obj)).map_err(|e| format!("Canal error: {}", e).into())
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
        TableSchema::new("users", vec![
            ColumnDef::new("id".to_string(), ColumnType::Int64),
            ColumnDef::new("name".to_string(), ColumnType::String),
        ])
    }
    #[test]
    fn test_canal_insert() {
        let rows = deserialize(b"{\"_type\":\"INSERT\",\"_ts\":1000,\"data\":[{\"id\":1,\"name\":\"alice\"}]}", &make_schema())
.unwrap();
        let row = &rows[0];
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(1));
    }
    #[test]
    fn test_canal_update() {
        let rows = deserialize(b"{\"_type\":\"UPDATE\",\"data\":[{\"id\":1,\"name\":\"new\"}],\"old\":[{\"id\":1,\"name\":\"old\"}]}", &make_schema())
.unwrap();
        let row = &rows[0];
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, seatunnel_api::RowKind::UpdateBefore);
        assert_eq!(rows[1].kind, seatunnel_api::RowKind::UpdateAfter);
    }
    #[test]
    fn test_canal_delete() {
        let rows = deserialize(b"{\"_type\":\"DELETE\",\"data\":[{\"id\":2}]}", &make_schema())
.unwrap();
        let row = &rows[0];
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, seatunnel_api::RowKind::Delete);
    }
}
