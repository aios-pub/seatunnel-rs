use seatunnel_api::{Row, TableSchema};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Row, Box<dyn Error>> {
    if bytes.len() < 5 { return Err("Avro too short".into()); }
    let payload = &bytes[5..];
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    let mut pos = 0;
    for (i, col) in schema.columns.iter().enumerate() {
        if pos >= payload.len() { row.set(i, seatunnel_api::Field::Null); continue; }
        let (field, consumed) = match col.column_type {
            seatunnel_api::ColumnType::Int64 => {
                if pos + 8 > payload.len() { (seatunnel_api::Field::Null, 0) }
                else { (seatunnel_api::Field::Int64(i64::from_le_bytes([payload[pos], payload[pos+1], payload[pos+2], payload[pos+3], payload[pos+4], payload[pos+5], payload[pos+6], payload[pos+7]])), 8) }
            }
            _ => {
                if pos + 4 > payload.len() { (seatunnel_api::Field::Null, 0) }
                else {
                    let len = u32::from_le_bytes([payload[pos], payload[pos+1], payload[pos+2], payload[pos+3]]) as usize;
                    pos += 4;
                    if pos + len > payload.len() { (seatunnel_api::Field::Null, 0) }
                    else { (seatunnel_api::Field::String(String::from_utf8_lossy(&payload[pos..pos+len]).to_string()), len) }
                }
            }
        };
        row.set(i, field);
        pos += consumed;
    }
    Ok(row)
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf: Vec<u8> = Vec::with_capacity(5 + 64);
    buf.push(0x00);
    buf.extend_from_slice(&1u32.to_be_bytes());
    for (i, col) in schema.columns.iter().enumerate() {
        if i >= row.field_count() { continue; }
        let field = row.get(i);
        match col.column_type {
            seatunnel_api::ColumnType::Int64 => {
                if let seatunnel_api::Field::Int64(v) = field { buf.extend_from_slice(&v.to_le_bytes()); }
            }
            _ => {
                let s = field_to_string(field);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }
    Ok(buf)
}

fn field_to_string(field: &seatunnel_api::Field) -> String {
    match field {
        seatunnel_api::Field::Null => String::new(),
        seatunnel_api::Field::String(s) => s.clone(),
        _ => format!("{:?}", field),
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
    fn test_avro_roundtrip() {
        let schema = make_schema();
        let mut row = Row::new(seatunnel_api::RowKind::Insert, 2);
        row.set(0, seatunnel_api::Field::Int64(42));
        row.set(1, seatunnel_api::Field::String("hello".to_string()));
        let bytes = serialize(&schema, &row).unwrap();
        assert_eq!(bytes[0], 0x00);
        let decoded = deserialize(&bytes, &schema).unwrap();
        assert_eq!(*decoded.get(0), seatunnel_api::Field::Int64(42));
        assert_eq!(*decoded.get(1), seatunnel_api::Field::String("hello".to_string()));
    }
    #[test]
    fn test_avro_invalid() {
        assert!(deserialize(b"sh", &make_schema()).is_err());
    }
}
