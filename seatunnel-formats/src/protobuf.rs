use seatunnel_api::{Row, TableSchema};
use std::error::Error;

pub fn deserialize(bytes: &[u8], schema: &TableSchema) -> Result<Vec<Row>, Box<dyn Error>> {
    let mut row = Row::new(seatunnel_api::RowKind::Insert, schema.column_count());
    let mut pos = 0;
    while pos < bytes.len() {
        let (tag, consumed) = read_varint(bytes, pos)?;
        pos += consumed;
        let wire_type = (tag & 7) as u8;
        let field_num = tag >> 3;
        if pos >= bytes.len() { break; }
        let i = field_num as usize - 1;
        if i >= schema.columns.len() { break; }
        let col = &schema.columns[i];
        let (field, consumed) = match wire_type {
            0 => {
                let (v, c) = read_varint(bytes, pos)?;
                let f = match col.column_type {
                    seatunnel_api::ColumnType::Int64 => seatunnel_api::Field::Int64(v as i64),
                    seatunnel_api::ColumnType::Bool => seatunnel_api::Field::Bool(v != 0),
                    _ => seatunnel_api::Field::Int64(v as i64),
                };
                (f, c)
            }
            2 => {
                let (len, c1) = read_varint(bytes, pos)?;
                let len = len as usize;
                let read_pos = pos + c1;
                if read_pos + len > bytes.len() { (seatunnel_api::Field::Null, 0) }
                else {
                    let s = String::from_utf8_lossy(&bytes[read_pos..read_pos+len]).to_string();
                    let f = match col.column_type {
                        seatunnel_api::ColumnType::Int64 => {
                            match s.parse::<i64>() { Ok(v) => seatunnel_api::Field::Int64(v), Err(_) => seatunnel_api::Field::String(s) }
                        }
                        seatunnel_api::ColumnType::Bool => {
                            match s.to_lowercase().as_str() { "true" | "1" => seatunnel_api::Field::Bool(true), _ => seatunnel_api::Field::Bool(false) }
                        }
                        _ => seatunnel_api::Field::String(s),
                    };
                    (f, c1 + len)
                }
            }
            _ => (seatunnel_api::Field::Null, 0),
        };
        row.set(i, field);
        pos += consumed;
    }
    Ok(vec![row])
}

pub fn serialize(schema: &TableSchema, row: &Row) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf = Vec::with_capacity(64);
    for (i, col) in schema.columns.iter().enumerate() {
        if i >= row.field_count() { continue; }
        let tag = ((i as u64 + 1) << 3) | 2u64;
        write_varint(&mut buf, tag);
        let s = field_to_string(row.get(i), &col.column_type);
        let bytes = s.as_bytes();
        write_varint(&mut buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }
    Ok(buf)
}

fn field_to_string(field: &seatunnel_api::Field, _col_type: &seatunnel_api::ColumnType) -> String {
    match field {
        seatunnel_api::Field::Null => String::new(),
        seatunnel_api::Field::String(s) => s.clone(),
        _ => field.to_string(),
    }
}

fn read_varint(bytes: &[u8], mut pos: usize) -> Result<(u64, usize), Box<dyn Error>> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut consumed = 0;
    loop {
        if pos >= bytes.len() { return Err("Truncated varint".into()); }
        let b = bytes[pos]; pos += 1; consumed += 1;
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 { break; }
        shift += 7;
    }
    Ok((result, consumed))
}

fn write_varint(buf: &mut Vec<u8>, mut val: u64) {
    while val > 0x7F { buf.push((val & 0x7F) as u8 | 0x80); val >>= 7; }
    buf.push(val as u8);
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
    fn test_protobuf_roundtrip() {
        let schema = make_schema();
        let mut row = Row::new(seatunnel_api::RowKind::Insert, 2);
        row.set(0, seatunnel_api::Field::Int64(42));
        row.set(1, seatunnel_api::Field::String("hello".to_string()));
        let bytes = serialize(&schema, &row)
.unwrap();

        let rows = deserialize(&bytes, &schema)
.unwrap();

        assert_eq!(*rows[0].get(0), seatunnel_api::Field::Int64(42));
        assert_eq!(*rows[0].get(1), seatunnel_api::Field::String("hello".to_string()));
    }
}
