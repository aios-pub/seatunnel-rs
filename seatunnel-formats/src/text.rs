use seatunnel_api::{Row, TableSchema};
use std::error::Error;

pub const DEFAULT_DELIMITER: &str = "|";

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    let text = std::str::from_utf8(bytes)?.trim_end_matches('\n').trim_end_matches('\r');
    let fields: Vec<&str> = text.split(DEFAULT_DELIMITER).collect();
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    for (i, col) in schema.columns.iter().enumerate() {
        if i < fields.len() && !fields[i].is_empty() {
            row.set(i, parse_text(fields[i], &col.column_type)?);
        }
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let parts: Vec<String> = schema.columns.iter().enumerate().map(|(i, col)| {
        if i < row.field_count() {
            let field = row.get(i);
            match field {
                seatunnel_api::Field::Null => String::new(),
                seatunnel_api::Field::String(s) => s.clone(),
                _ => format!("{}", field),
            }
        } else {
            String::new()
        }
    }).collect();
    Ok(parts.join(DEFAULT_DELIMITER).into_bytes())
}

fn parse_text(s: &str, col_type: &seatunnel_api::ColumnType) -> Result<seatunnel_api::Field, Box<dyn Error>> {
    match col_type {
        seatunnel_api::ColumnType::Int32 => Ok(seatunnel_api::Field::Int32(s.parse()?)),
        seatunnel_api::ColumnType::Int64 => Ok(seatunnel_api::Field::Int64(s.parse()?)),
        seatunnel_api::ColumnType::Float64 => Ok(seatunnel_api::Field::Float64(s.parse()?)),
        seatunnel_api::ColumnType::Bool => {
            match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(seatunnel_api::Field::Bool(true)),
                "false" | "0" | "no" => Ok(seatunnel_api::Field::Bool(false)),
                _ => Ok(seatunnel_api::Field::String(s.to_string())),
            }
        }
        _ => Ok(seatunnel_api::Field::String(s.to_string())),
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
    fn test_text_roundtrip() {
        let schema = make_schema();
        let mut row = Row::new(seatunnel_api::RowKind::Insert, 2);
        row.set(0, seatunnel_api::Field::Int64(7));
        row.set(1, seatunnel_api::Field::String("test".to_string()));
        assert_eq!(serialize(&schema, &row).unwrap(), b"7|test");
        let decoded = deserialize(b"42|hello", &schema).unwrap();
        assert_eq!(*decoded.get(0), seatunnel_api::Field::Int64(42));
        assert_eq!(*decoded.get(1), seatunnel_api::Field::String("hello".to_string()));
    }
}
