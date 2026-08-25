use seatunnel_api::{Row, TableSchema};
use serde_json::{Value};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(bytes)?;
    let obj = value.as_object().ok_or("Expected JSON object")?;
    let kind = match obj.get("OP_TYPE").and_then(|v| v.as_str()) {
        Some("INSERT") => seatunnel_api::RowKind::Insert,
        Some("DELETE") => seatunnel_api::RowKind::Delete,
        Some("UPDATE") => seatunnel_api::RowKind::UpdateAfter,
        _ => seatunnel_api::RowKind::Insert,
    };
    let empty_arr: Vec<Value> = Vec::new();
    let after = match obj.get("AFTER_VALUE").and_then(|v| v.as_array()) { Some(a) => a, None => &empty_arr };
    let cols = match obj.get("COLUMN_NAME").and_then(|v| v.as_array()) { Some(a) => a, None => &empty_arr };
    let mut row = Row::new(kind, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        let idx = (0..cols.len()).find(|&j| cols[j].as_str().map_or(false, |s| s.eq_ignore_ascii_case(&col.name)));
        let field = match idx {
            Some(j) if j < after.len() => json_to_field(Some(&after[j]))?,
            _ => json_to_field(None)?,
        };
        row.set(i, field);
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut obj = serde_json::Map::<String, Value>::default();
    obj.insert("OP_TYPE".to_string(), Value::String(match row.kind { seatunnel_api::RowKind::Delete => "DELETE".to_string(), _ => "INSERT".to_string() }));
    obj.insert("TABLE_NAME".to_string(), Value::String(schema.table_identifier.clone()));
    let col_names: Vec<Value> = schema.columns.iter().map(|c| Value::String(c.name.clone())).collect();
    let after_vals: Vec<Value> = schema.columns.iter().enumerate().map(|(i, _)| {
        if i < row.field_count() { field_to_json(row.get(i)).unwrap_or(Value::Null) } else { Value::Null }
    }).collect();
    obj.insert("COLUMN_NAME".to_string(), Value::Array(col_names));
    obj.insert("AFTER_VALUE".to_string(), Value::Array(after_vals));
    serde_json::to_vec(&Value::Object(obj)).map_err(|e| format!("OGG error: {}", e).into())
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
    fn test_ogg_insert() {
        let row = deserialize(b"{\"OP_TYPE\":\"INSERT\",\"COLUMN_NAME\":[\"id\",\"name\"],\"AFTER_VALUE\":[1,\"alice\"]}", &make_schema()).unwrap();
        assert_eq!(row.kind, seatunnel_api::RowKind::Insert);
        assert_eq!(*row.get(0), seatunnel_api::Field::Int64(1));
    }
}
