//! TiDB row encoding/decoding utilities.
//!
//! Decodes TiKV record keys (`t{table_id}_r{handle}`) and the chunk-format
//! row values emitted by TiDB, plus Percolator transaction correlation.

use std::collections::HashMap;

use crate::kvproto::cdcpb::event::Row as CdcRow;

/// Table record prefix byte: 't'.
const TABLE_PREFIX: u8 = b't';
/// Record key separator: 'r'.
const RECORD_PREFIX: u8 = b'r';
/// Index key separator: 'i'.
#[allow(dead_code)] // part of the documented TiDB key encoding
const INDEX_PREFIX: u8 = b'i';

/// Row format version markers.
const ROW_V1: u8 = 0x00;
const ROW_V2: u8 = 0x01;

/// Column encoding flags (TiDB chunk / OLTP row format).
const FLAG_VARINT: u8 = 1;
const FLAG_UVARINT: u8 = 2;
const FLAG_FLOAT32: u8 = 3;
const FLAG_FLOAT64: u8 = 4;
const FLAG_BYTES: u8 = 5;
const FLAG_COMPACT_BYTES: u8 = 6;
const FLAG_DECIMAL: u8 = 8;
const FLAG_VARINT_COMPAT: u8 = 9;
const FLAG_UVARINT_COMPAT: u8 = 10;
const FLAG_JSON: u8 = 11;
const FLAG_NULL: u8 = 12;
const FLAG_BYTES_LEN_ENCODED: u8 = 13;

/// A decoded row: column index -> raw bytes (still lazily decoded).
#[derive(Debug, Clone)]
pub struct DecodedRow {
    pub table_id: i64,
    pub handle: i64,
    pub columns: HashMap<i32, ColumnValue>,
}

/// A single decoded column value.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    Bytes(Vec<u8>),
    Text(String),
    Json(String),
}

/// Decode a TiKV record key into (table_id, handle).
///
/// Format: `t<8-byte table_id>_r<handle>` where handle is either an int64
/// (big-endian) or an encoded key for clustered tables.
/// Sign-flipped big-endian encoding used for i64 components in TiKV keys
/// (mirrors `codec.EncodeIntToCmpUint`); exposed for tests.
#[cfg(test)]
pub(crate) fn encode_cmp_u64_for_test(v: i64) -> [u8; 8] {
    (v as u64 ^ (1 << 63)).to_be_bytes()
}

pub fn decode_record_key(key: &[u8]) -> Option<(i64, i64)> {
    // Layout: 't' + 8B cmp_i64(table_id) + "_r" + 8B cmp_i64(handle)
    if key.len() < 19 || key[0] != TABLE_PREFIX || key[9] != b'_' || key[10] != RECORD_PREFIX {
        return None;
    }
    // Table id and int handles are stored sign-bit-flipped (cmpuint).
    let table_id = (u64::from_be_bytes(key[1..9].try_into().ok()?) ^ (1 << 63)) as i64;
    let handle = decode_handle(&key[11..])?;
    Some((table_id, handle))
}

/// Decode an int64 handle from the tail of a record key.
fn decode_handle(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 8 {
        // cmpuint int handle: undo the sign-bit flip.
        return Some((u64::from_be_bytes(bytes.try_into().ok()?) ^ (1 << 63)) as i64);
    }
    // Encoded key handles: first byte is the compact-key marker (0x00 = int).
    if bytes.len() > 8 && bytes[0] == 0x00 {
        return Some((u64::from_be_bytes(bytes[1..9].try_into().ok()?) ^ (1 << 63)) as i64);
    }
    None
}

/// Parse the columns of a TiDB row value (chunk format, version 1/2).
///
/// Row format v2: `0x01 <varint len> <columns>`
/// Row format v1: `0x00 <columns>`
/// Each column: `<flag> <data>`
pub fn decode_row_value(value: &[u8]) -> Result<Vec<ColumnValue>, String> {
    if value.is_empty() {
        return Err("empty row value".into());
    }
    let (body, version) = match value[0] {
        ROW_V1 => (&value[1..], 1),
        ROW_V2 => {
            // varint encoded body length (unused for parsing)
            let (len, consumed) = decode_varint(&value[1..])?;
            let _ = len;
            (&value[1 + consumed..], 2)
        }
        other => return Err(format!("unknown row version {:02x}", other)),
    };
    let _ = version;
    let mut columns = Vec::new();
    let mut i = 0;
    while i < body.len() {
        let flag = body[i];
        i += 1;
        match flag {
            FLAG_NULL => columns.push(ColumnValue::Null),
            FLAG_VARINT | FLAG_VARINT_COMPAT => {
                let (v, consumed) = decode_varint(&body[i..])?;
                i += consumed;
                columns.push(ColumnValue::Int(v));
            }
            FLAG_UVARINT | FLAG_UVARINT_COMPAT => {
                let (v, consumed) = decode_uvarint(&body[i..])?;
                i += consumed;
                columns.push(ColumnValue::UInt(v));
            }
            FLAG_FLOAT32 => {
                if i + 4 > body.len() {
                    return Err("truncated float32".into());
                }
                let b: [u8; 4] = body[i..i + 4].try_into().unwrap();
                i += 4;
                columns.push(ColumnValue::Float(f32::from_be_bytes(b) as f64));
            }
            FLAG_FLOAT64 => {
                if i + 8 > body.len() {
                    return Err("truncated float64".into());
                }
                let b: [u8; 8] = body[i..i + 8].try_into().unwrap();
                i += 8;
                columns.push(ColumnValue::Float(f64::from_be_bytes(b)));
            }
            FLAG_BYTES | FLAG_COMPACT_BYTES | FLAG_BYTES_LEN_ENCODED => {
                let (len, consumed) = decode_uvarint(&body[i..])?;
                i += consumed;
                if i + len as usize > body.len() {
                    return Err("truncated bytes".into());
                }
                let data = body[i..i + len as usize].to_vec();
                i += len as usize;
                columns.push(ColumnValue::Bytes(data));
            }
            FLAG_DECIMAL => {
                // decimal: precision/scale mode byte + uvarint bytes
                if i >= body.len() {
                    return Err("truncated decimal".into());
                }
                // Let it fall through to bytes-like handling.
                let (len, consumed) = decode_uvarint(&body[i..])?;
                i += consumed;
                if i + len as usize > body.len() {
                    return Err("truncated decimal data".into());
                }
                let data = body[i..i + len as usize].to_vec();
                i += len as usize;
                columns.push(ColumnValue::Text(
                    String::from_utf8_lossy(&data).into_owned(),
                ));
            }
            FLAG_JSON => {
                let (len, consumed) = decode_uvarint(&body[i..])?;
                i += consumed;
                if i + len as usize > body.len() {
                    return Err("truncated json".into());
                }
                let data = body[i..i + len as usize].to_vec();
                i += len as usize;
                columns.push(ColumnValue::Json(
                    String::from_utf8_lossy(&data).into_owned(),
                ));
            }
            other => {
                return Err(format!("unknown column flag {:02x}", other));
            }
        }
    }
    Ok(columns)
}

/// Decode a TiDB varint.
///
/// TiDB's `codec.EncodeInt` uses a sign-preserving comparable encoding:
/// positive values encode as their uvarint, negative values get a leading
/// 0xFF marker. The common inline (0..=250) form is the raw byte value.
fn decode_varint(bytes: &[u8]) -> Result<(i64, usize), String> {
    if bytes.is_empty() {
        return Err("empty varint".into());
    }
    let first = bytes[0];
    // Negative marker: 0xFF followed by the uvarint of the magnitude.
    if first == 0xFF {
        let (u, n) = decode_uvarint(&bytes[1..])?;
        return Ok((-(u as i64), n + 1));
    }
    let (u, n) = decode_uvarint(bytes)?;
    Ok((u as i64, n))
}

/// Decode a TiDB uvarint (big-endian, first byte is length).
///
/// TiDB encodes uvarints as: `<len_byte> <bytes>` where len_byte prefixes
/// (value 0..=250 encodes inline; 251..255 signal extended lengths). For
/// simplicity we handle the common inline and 8-byte cases.
fn decode_uvarint(bytes: &[u8]) -> Result<(u64, usize), String> {
    if bytes.is_empty() {
        return Err("empty uvarint".into());
    }
    let first = bytes[0] as usize;
    if first == 252 {
        // 2-byte length
        if bytes.len() < 3 {
            return Err("truncated uvarint len".into());
        }
        let n = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
        Ok((n as u64, 3))
    } else if first == 253 {
        if bytes.len() < 5 {
            return Err("truncated uvarint len".into());
        }
        let n = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        Ok((n as u64, 5))
    } else if first == 254 {
        let n = u64::from_be_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| "truncated uvarint".to_string())?,
        );
        Ok((n, 9))
    } else if first <= 250 {
        Ok((first as u64, 1))
    } else {
        Err(format!("unsupported uvarint prefix {}", first))
    }
}

/// Percolator transaction correlation key: (row handle, start_ts).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowKeyWithTs {
    pub key: u64,
    pub ts: u64,
}

impl RowKeyWithTs {
    pub fn of_start(handle: i64, start_ts: u64) -> Self {
        RowKeyWithTs {
            key: handle as u64,
            ts: start_ts,
        }
    }
    pub fn of_commit(handle: i64, commit_ts: u64) -> Self {
        RowKeyWithTs {
            key: handle as u64,
            ts: commit_ts,
        }
    }
}

/// A pending prewrite/commit row awaiting committed-ts resolution.
#[derive(Debug, Clone)]
pub struct PendingRow {
    pub handle: i64,
    pub start_ts: u64,
    pub commit_ts: u64,
    pub value: Vec<u8>,
    pub old_value: Vec<u8>,
    pub op_type: i32,
}

/// Percolator prewrite/commit correlation engine.
///
/// Mirrors Java's TiDBSourceReader: track prewrites and commits keyed by
/// (row, ts), then flush rows whose commit_ts <= resolved_ts.
#[derive(Debug, Default)]
pub struct TransactionTracker {
    /// (handle, start_ts) -> prewrite row.
    prewrites: HashMap<RowKeyWithTs, PendingRow>,
    /// (handle, commit_ts) -> committed row (awaiting flush).
    commits: HashMap<RowKeyWithTs, PendingRow>,
}

impl TransactionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a CDC row event from the EventFeed stream.
    pub fn on_row(&mut self, row: &CdcRow) {
        tracing::debug!(
            "TiKV CDC tracker: on_row type={} key_len={} start_ts={} commit_ts={}",
            row.r#type,
            row.key.len(),
            row.start_ts,
            row.commit_ts
        );
        let Some((_table_id, handle)) = decode_record_key(&row.key) else {
            tracing::debug!(
                "TiKV CDC tracker: failed to decode record key: {}",
                row.key
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            );
            return;
        };
        // LogType: PREWRITE=1, COMMIT=2, ROLLBACK=3, COMMITTED=4
        let pending = PendingRow {
            handle,
            start_ts: row.start_ts,
            commit_ts: row.commit_ts,
            value: row.value.clone(),
            old_value: row.old_value.clone(),
            op_type: row.op_type,
        };
        match row.r#type {
            1 => {
                // PREWRITE
                self.prewrites
                    .insert(RowKeyWithTs::of_start(handle, row.start_ts), pending);
            }
            2 => {
                // COMMIT
                self.commits
                    .insert(RowKeyWithTs::of_commit(handle, row.commit_ts), pending);
            }
            3 => {
                // ROLLBACK — drop the prewrite
                self.prewrites
                    .remove(&RowKeyWithTs::of_start(handle, row.start_ts));
            }
            4 => {
                // COMMITTED — carries full row; register both prewrite and commit
                self.prewrites.insert(
                    RowKeyWithTs::of_start(handle, row.start_ts),
                    pending.clone(),
                );
                self.commits.insert(
                    RowKeyWithTs::of_commit(handle, row.commit_ts),
                    pending.clone(),
                );
            }
            _ => {}
        }
    }

    /// Flush rows whose commit_ts <= resolved_ts (safe barrier).
    /// Returns emitted rows in commit order.
    pub fn flush(&mut self, resolved_ts: u64) -> Vec<PendingRow> {
        let mut ready: Vec<(u64, PendingRow)> = Vec::new();
        let mut to_remove = Vec::new();
        for (k, commit) in self.commits.iter() {
            if commit.commit_ts <= resolved_ts {
                // Find the matching prewrite by handle + start_ts
                if let Some(prewrite) = self
                    .prewrites
                    .get(&RowKeyWithTs::of_start(commit.handle, commit.start_ts))
                {
                    let mut full = prewrite.clone();
                    full.commit_ts = commit.commit_ts;
                    ready.push((commit.commit_ts, full));
                }
                to_remove.push(*k);
            }
        }
        for k in to_remove {
            self.commits.remove(&k);
        }
        // Remove flushed prewrites
        for (_, row) in &ready {
            self.prewrites
                .remove(&RowKeyWithTs::of_start(row.handle, row.start_ts));
        }
        ready.sort_by_key(|(ts, _)| *ts);
        ready.into_iter().map(|(_, r)| r).collect()
    }

    /// Number of in-flight transactions (for diagnostics).
    pub fn pending_count(&self) -> usize {
        self.prewrites.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_record_key_int_handle() {
        // Real TiDB keys store table id and int handles sign-bit-flipped.
        let mut key = Vec::new();
        key.push(b't');
        key.extend_from_slice(&encode_cmp_u64_for_test(100));
        key.push(b'_');
        key.push(b'r');
        key.extend_from_slice(&encode_cmp_u64_for_test(42));
        let (table_id, handle) = decode_record_key(&key).unwrap();
        assert_eq!(table_id, 100);
        assert_eq!(handle, 42);

        // A plain-BE encoding decodes to garbage (negative ids) — it must
        // never be produced by the connector's own range builder.
        assert_ne!(encode_cmp_u64_for_test(100), 100i64.to_be_bytes());
    }

    #[test]
    fn test_decode_record_key_rejects_index() {
        let mut key = Vec::new();
        key.push(b't');
        key.extend_from_slice(&100i64.to_be_bytes());
        key.push(b'i'); // index key
        key.extend_from_slice(&42i64.to_be_bytes());
        assert!(decode_record_key(&key).is_none());
    }

    #[test]
    fn test_decode_row_value_v2() {
        // v2: 0x01, length prefix 0x02 (body len=len bytes), then columns
        // column 0: flag 1 (varint) value 5 -> encoded as uvarint inline 5
        let mut value = vec![ROW_V2, 0x02];
        value.push(FLAG_VARINT);
        value.push(5);
        let cols = decode_row_value(&value).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0], ColumnValue::Int(5));
    }

    #[test]
    fn test_decode_row_value_null_and_bytes() {
        let mut value = vec![ROW_V1];
        value.push(FLAG_NULL);
        value.push(FLAG_BYTES);
        value.push(3); // length
        value.extend_from_slice(b"abc");
        let cols = decode_row_value(&value).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ColumnValue::Null);
        assert_eq!(cols[1], ColumnValue::Bytes(b"abc".to_vec()));
    }

    #[test]
    fn test_transaction_tracker_commit_flow() {
        let mut tracker = TransactionTracker::new();
        // COMMITTED row: handle 5, start_ts 100, commit_ts 200
        let row = CdcRow {
            start_ts: 100,
            commit_ts: 200,
            r#type: 4,
            op_type: 1, // PUT
            key: {
                let mut k = Vec::new();
                k.push(b't');
                k.extend_from_slice(&encode_cmp_u64_for_test(1));
                k.push(b'_');
                k.push(b'r');
                k.extend_from_slice(&encode_cmp_u64_for_test(5));
                k
            },
            value: vec![1, 2, 3],
            old_value: vec![],
            ..Default::default()
        };
        tracker.on_row(&row);
        // Nothing flushes before resolved_ts reaches 200
        assert!(tracker.flush(150).is_empty());
        let emitted = tracker.flush(200);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].handle, 5);
        assert_eq!(emitted[0].commit_ts, 200);
    }
}
