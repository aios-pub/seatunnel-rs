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

/// Column type classification used to decode `rowcodec` v2 values (which
/// are untyped byte spans and need the table schema to interpret).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowColType {
    /// Integer family (tinyint..bigint); `unsigned` widens the domain.
    Int { unsigned: bool },
    /// Float/double.
    Float,
    /// Character strings — stored verbatim in the row.
    String,
    /// Everything else (decimal, temporal, json, bit, enum, ...) — passed
    /// through as raw bytes for now.
    Other,
}

/// Classify a `COLUMN_TYPE` string from information_schema.
pub fn parse_column_type(column_type: &str) -> RowColType {
    let ct = column_type.trim().to_lowercase();
    let unsigned = ct.ends_with("unsigned");
    if ct.starts_with("tinyint")
        || ct.starts_with("smallint")
        || ct.starts_with("mediumint")
        || ct.starts_with("int")
        || ct.starts_with("bigint")
    {
        RowColType::Int { unsigned }
    } else if ct.starts_with("float") || ct.starts_with("double") {
        RowColType::Float
    } else if ct.starts_with("char") || ct.starts_with("varchar") || ct.starts_with("text") {
        RowColType::String
    } else {
        RowColType::Other
    }
}

/// Decode a TiDB row value using the table's column types (ordered by
/// ordinal position). Handles both the legacy "chunk" format (v1/v2 marker
/// byte 0x00/0x01, self-describing) and the `rowcodec` v2 format (marker
/// 0x80, the default since TiDB row_format_version=2).
///
/// For rowcodec v2 the clustered-table primary key column (absent from the
/// value) is filled from `handle` when it is an integer column at
/// `pk_ordinal`.
pub fn decode_row_value_with_schema(
    value: &[u8],
    columns: &[RowColType],
    pk_ordinal: Option<usize>,
    handle: i64,
) -> Result<Vec<ColumnValue>, String> {
    if value.first() == Some(&0x80) {
        decode_rowcodec_v2(value, columns, pk_ordinal, handle)
    } else {
        // Legacy chunk format is self-describing; positional output.
        decode_row_value(value)
    }
}

/// TiDB `rowcodec` v2 layout (pkg/util/rowcodec/row.go):
/// `VER(0x80) | flags | num_not_null(u16 LE) | num_null(u16 LE)
///  | col_ids (large? u32 LE : u8) x (num_not_null+num_null)
///  | offsets (large? u32 LE : u16 LE) x num_not_null   // cumulative ends
///  | data (offsets.last bytes)
///  | optional checksum (flags & 0x02)`
/// flags & 0x01 = large (col id > 255 or row data > 64KB).
fn decode_rowcodec_v2(
    value: &[u8],
    columns: &[RowColType],
    pk_ordinal: Option<usize>,
    handle: i64,
) -> Result<Vec<ColumnValue>, String> {
    if value.len() < 6 {
        return Err("rowcodec v2 header truncated".into());
    }
    let flags = value[1];
    let large = flags & 0x01 != 0;
    let num_not_null = u16::from_le_bytes([value[2], value[3]]) as usize;
    let num_null = u16::from_le_bytes([value[4], value[5]]) as usize;
    let id_width = if large { 4 } else { 1 };
    let off_width = if large { 4 } else { 2 };

    let need = 6 + (num_not_null + num_null) * id_width + num_not_null * off_width;
    if value.len() < need {
        return Err("rowcodec v2 column metadata truncated".into());
    }
    let mut cursor = 6;
    let read_id = |cursor: &mut usize| -> u64 {
        let id = if large {
            u32::from_le_bytes(value[*cursor..*cursor + 4].try_into().unwrap()) as u64
        } else {
            value[*cursor] as u64
        };
        *cursor += id_width;
        id
    };
    let mut not_null_ids = Vec::with_capacity(num_not_null);
    for _ in 0..num_not_null {
        not_null_ids.push(read_id(&mut cursor));
    }
    let mut null_ids = Vec::with_capacity(num_null);
    for _ in 0..num_null {
        null_ids.push(read_id(&mut cursor));
    }
    let mut offsets = Vec::with_capacity(num_not_null);
    for _ in 0..num_not_null {
        let off = if large {
            u32::from_le_bytes(value[cursor..cursor + 4].try_into().unwrap()) as usize
        } else {
            u16::from_le_bytes([value[cursor], value[cursor + 1]]) as usize
        };
        cursor += off_width;
        offsets.push(off);
    }
    let data_len = offsets.last().copied().unwrap_or(0);
    if value.len() < cursor + data_len {
        return Err("rowcodec v2 data truncated".into());
    }
    let data = &value[cursor..cursor + data_len];

    // rowcodec column ids are the TiDB column offsets; for tables created
    // without dropped columns they equal the 1-based ordinal position.
    let mut out = vec![ColumnValue::Null; columns.len()];
    let mut prev_end = 0usize;
    for (i, id) in not_null_ids.iter().enumerate() {
        let end = offsets[i];
        let idx = usize::try_from(*id).ok().and_then(|v| v.checked_sub(1));
        let col = idx
            .and_then(|i| columns.get(i))
            .copied()
            .unwrap_or(RowColType::Other);
        if let Some(i) = idx.filter(|i| *i < out.len()) {
            out[i] = decode_typed_value(&data[prev_end..end], col);
        }
        prev_end = end;
    }
    for id in &null_ids {
        if let Some(i) = usize::try_from(*id)
            .ok()
            .and_then(|v| v.checked_sub(1))
            .filter(|i| *i < out.len())
        {
            out[i] = ColumnValue::Null;
        }
    }
    // Clustered int primary key lives in the record key, not the value.
    if let Some(pk) = pk_ordinal {
        if pk >= 1
            && pk <= out.len()
            && matches!(out[pk - 1], ColumnValue::Null)
            && matches!(columns[pk - 1], RowColType::Int { .. })
        {
            out[pk - 1] = ColumnValue::Int(handle);
        }
    }
    Ok(out)
}

/// Decode one rowcodec v2 column payload. Integer columns use TiDB's
/// compact encoding: minimum-width (1/2/4/8 byte) two's-complement
/// little-endian.
fn decode_typed_value(data: &[u8], col: RowColType) -> ColumnValue {
    match col {
        RowColType::Int { unsigned } => {
            let v = decode_compact_int(data);
            if unsigned {
                ColumnValue::UInt(v as u64)
            } else {
                ColumnValue::Int(v)
            }
        }
        RowColType::Float => {
            if data.len() == 8 {
                ColumnValue::Float(f64::from_be_bytes(data.try_into().unwrap()))
            } else {
                ColumnValue::Bytes(data.to_vec())
            }
        }
        RowColType::String => match std::str::from_utf8(data) {
            Ok(s) => ColumnValue::Text(s.to_string()),
            Err(_) => ColumnValue::Bytes(data.to_vec()),
        },
        RowColType::Other => ColumnValue::Bytes(data.to_vec()),
    }
}

fn decode_compact_int(data: &[u8]) -> i64 {
    match data.len() {
        1 => data[0] as i8 as i64,
        2 => i16::from_le_bytes(data.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(data.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(data.try_into().unwrap()),
        _ => 0,
    }
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

/// Encode a raw key into the memcomparable "chunked" form used by PD region
/// boundaries and CDC span keys (what official TiCDC's
/// `spanz.ToComparableKey` sends). Every 8-byte chunk is emitted verbatim,
/// padded with zeros to 8 bytes and terminated with `0xFF - pad_count`, so a
/// key with more real bytes in its last chunk sorts higher.
///
/// Spans sent to TiKV `ChangeDataRequest` — and region-range queries sent to
/// PD — MUST use this form: PD region boundaries are stored encoded, so raw
/// keys compare against them incorrectly and resolve to the wrong regions
/// (e.g. a raw table span matches the wide meta region instead of the actual
/// data regions, and no PREWRITE/COMMIT delta ever arrives).
pub fn encode_comparable(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() / 8 * 9 + 9);
    for chunk in key.chunks(8) {
        let pad = 8 - chunk.len();
        out.extend_from_slice(chunk);
        out.extend(std::iter::repeat_n(0u8, pad));
        out.push(0xFF - pad as u8);
    }
    out
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
/// Mirrors official TiCDC's `txn_matcher` semantics: track prewrites and
/// commits keyed by (row, ts), then flush rows whose commit_ts <= resolved_ts.
/// Before a region's INITIALIZED marker, commits/rollbacks without a seen
/// prewrite are cached (prewrites can be delivered out of order during the
/// scan phase) and replayed once the marker arrives.
#[derive(Debug, Default)]
pub struct TransactionTracker {
    /// (handle, start_ts) -> prewrite row.
    prewrites: HashMap<RowKeyWithTs, PendingRow>,
    /// (handle, commit_ts) -> committed row (awaiting flush).
    commits: HashMap<RowKeyWithTs, PendingRow>,
    /// Regions whose INITIALIZED marker (Row.LogType = 5) has been seen.
    initialized_regions: std::collections::HashSet<u64>,
    /// Commits that arrived before INITIALIZED without a matching prewrite.
    cached_commits: HashMap<u64, Vec<PendingRow>>,
    /// Rollbacks that arrived before INITIALIZED without a seen prewrite.
    cached_rollbacks: HashMap<u64, Vec<(i64, u64)>>,
}

impl TransactionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a CDC row event from the EventFeed stream.
    pub fn on_row(&mut self, region_id: u64, row: &CdcRow) {
        tracing::debug!(
            "TiKV CDC tracker: on_row region={} type={} key_len={} start_ts={} commit_ts={}",
            region_id,
            row.r#type,
            row.key.len(),
            row.start_ts,
            row.commit_ts
        );
        // LogType: PREWRITE=1, COMMIT=2, ROLLBACK=3, COMMITTED=4,
        // INITIALIZED=5.
        if row.r#type == 5 {
            self.mark_initialized(region_id);
            return;
        }
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
        let pending = PendingRow {
            handle,
            start_ts: row.start_ts,
            commit_ts: row.commit_ts,
            value: row.value.clone(),
            old_value: row.old_value.clone(),
            op_type: row.op_type,
        };
        let initialized = self.initialized_regions.contains(&region_id);
        match row.r#type {
            1 => {
                // PREWRITE. TiKV may send a fake prewrite with an empty value
                // (txn heartbeat); never let it overwrite a real prewrite.
                let key = RowKeyWithTs::of_start(handle, row.start_ts);
                let overwrite = match self.prewrites.get(&key) {
                    Some(existing) => existing.value.is_empty(),
                    None => true,
                };
                if overwrite {
                    self.prewrites.insert(key, pending);
                }
            }
            2 => {
                // COMMIT carries key + timestamps; the value comes from the
                // matched prewrite. Before INITIALIZED a missing prewrite is
                // expected (scan-phase reordering) — cache and replay later.
                let start_key = RowKeyWithTs::of_start(handle, row.start_ts);
                // Pipelined-DML (generation > 0) commits only match after
                // INITIALIZED, mirroring official TiCDC.
                let matched =
                    self.prewrites.contains_key(&start_key) && (initialized || row.generation == 0);
                if matched {
                    self.commits
                        .insert(RowKeyWithTs::of_commit(handle, row.commit_ts), pending);
                } else if !initialized {
                    self.cached_commits
                        .entry(region_id)
                        .or_default()
                        .push(pending);
                } else {
                    tracing::warn!(
                        "TiKV CDC tracker: commit without prewrite after initialized \
                         (region {}, handle {}, start_ts {}) — dropped",
                        region_id,
                        handle,
                        row.start_ts
                    );
                }
            }
            3 => {
                // ROLLBACK — drop the prewrite (cache it before INITIALIZED
                // because the prewrite may still be in flight).
                if self
                    .prewrites
                    .remove(&RowKeyWithTs::of_start(handle, row.start_ts))
                    .is_none()
                    && !initialized
                {
                    self.cached_rollbacks
                        .entry(region_id)
                        .or_default()
                        .push((handle, row.start_ts));
                }
            }
            4 => {
                // COMMITTED — carries the full row; register both sides.
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

    /// Latch a region's INITIALIZED marker and replay events that were
    /// cached while the region's scan was still converging.
    fn mark_initialized(&mut self, region_id: u64) {
        if !self.initialized_regions.insert(region_id) {
            return;
        }
        let cached_commits = self.cached_commits.remove(&region_id).unwrap_or_default();
        let cached_rollbacks = self.cached_rollbacks.remove(&region_id).unwrap_or_default();
        let mut unmatched = 0usize;
        for commit in cached_commits {
            let start_key = RowKeyWithTs::of_start(commit.handle, commit.start_ts);
            if self.prewrites.contains_key(&start_key) {
                self.commits.insert(
                    RowKeyWithTs::of_commit(commit.handle, commit.commit_ts),
                    commit,
                );
            } else {
                unmatched += 1;
            }
        }
        for (handle, start_ts) in cached_rollbacks {
            self.prewrites
                .remove(&RowKeyWithTs::of_start(handle, start_ts));
        }
        if unmatched > 0 {
            tracing::debug!(
                "TiKV CDC tracker: region {} initialized, {} cached commit(s) had no prewrite",
                region_id,
                unmatched
            );
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
    fn test_encode_comparable_matches_pd_region_boundaries() {
        // Vectors captured from a real TiDB v8.1 cluster: table 164 span
        // `t164_r..t164_s` raw vs the encoded form PD region boundaries and
        // official TiCDC subscriptions use.
        let (raw_start, raw_end) = (
            [0x74u8, 0x80, 0, 0, 0, 0, 0, 0, 0xA4, b'_', b'r'].as_slice(),
            [0x74u8, 0x80, 0, 0, 0, 0, 0, 0, 0xA4, b'_', b's'].as_slice(),
        );
        let enc_start = encode_comparable(raw_start);
        assert_eq!(hex_of(&enc_start), "7480000000000000ffa45f720000000000fa");
        assert_eq!(
            hex_of(&encode_comparable(raw_end)),
            "7480000000000000ffa45f730000000000fa"
        );

        // A 9-byte key (table prefix only) pads with 7 zeros -> 0xF8 marker,
        // matching the split boundary between table regions.
        let table_prefix = [0x74u8, 0x80, 0, 0, 0, 0, 0, 0, 0xA4].as_slice();
        assert_eq!(
            hex_of(&encode_comparable(table_prefix)),
            "7480000000000000ffa400000000000000f8"
        );

        // Exact 8-byte chunk gets a 0xFF marker with no padding.
        assert_eq!(
            hex_of(&encode_comparable(&[0x72u8, 0, 0, 1, 0, 0, 0, 0])),
            "7200000100000000ff"
        );
        // 4-byte meta key boundary observed in PD (`72000001...` region).
        assert_eq!(
            hex_of(&encode_comparable(&[0x72u8, 0, 0, 1])),
            "7200000100000000fb"
        );
        // Ordering property: longer real data in the final chunk sorts higher.
        assert!(encode_comparable(raw_start) < encode_comparable(raw_end));
    }

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Build a record key for `handle` in table 1.
    fn record_key(handle: i64) -> Vec<u8> {
        let mut k = Vec::new();
        k.push(b't');
        k.extend_from_slice(&encode_cmp_u64_for_test(1));
        k.push(b'_');
        k.push(b'r');
        k.extend_from_slice(&encode_cmp_u64_for_test(handle));
        k
    }

    fn from_hex(hex: &str) -> Vec<u8> {
        let cleaned: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        (0..cleaned.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&cleaned[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_decode_rowcodec_v2_captured_vectors() {
        // users(id int PK, name varchar, score int) — hex captured from
        // live TiDB v8.1 EventFeed COMMITTED rows.
        let columns = [
            RowColType::Int { unsigned: false },
            RowColType::String,
            RowColType::Int { unsigned: false },
        ];
        let decode = |hex: &str, handle: i64| {
            decode_row_value_with_schema(&from_hex(hex), &columns, Some(1), handle).unwrap()
        };

        // ('fmt_probe2', 88), handle 17 — clustered PK absent from value.
        let row = decode("8000020000000203 0a00 0b00 666d745f70726f626532 58", 17);
        assert_eq!(row[0], ColumnValue::Int(17));
        assert_eq!(row[1], ColumnValue::Text("fmt_probe2".into()));
        assert_eq!(row[2], ColumnValue::Int(88));

        // Compact integers: 63 inline i8, 250/1000 i16 LE, 100000 i32 LE, -5 i8.
        assert_eq!(
            decode("8000020000000203 0300 0400 6e3633 3f", 18)[2],
            ColumnValue::Int(63)
        );
        assert_eq!(
            decode("8000020000000203 0400 0600 6e323530 fa00", 20)[2],
            ColumnValue::Int(250)
        );
        assert_eq!(
            decode("8000020000000203 0500 0700 6e31303030 e803", 23)[2],
            ColumnValue::Int(1000)
        );
        assert_eq!(
            decode("8000020000000203 0700 0b00 6e313030303030 a0860100", 24)[2],
            ColumnValue::Int(100000)
        );
        assert_eq!(
            decode("8000020000000203 0300 0400 6e6567 fb", 25)[2],
            ColumnValue::Int(-5)
        );

        // score = NULL: the column is entirely absent from the value.
        let row = decode("80000100000002 0400 6e756c76", 26);
        assert_eq!(row[1], ColumnValue::Text("nulv".into()));
        assert_eq!(row[2], ColumnValue::Null);
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
            key: record_key(5),
            value: vec![1, 2, 3],
            old_value: vec![],
            ..Default::default()
        };
        tracker.on_row(7, &row);
        // Nothing flushes before resolved_ts reaches 200
        assert!(tracker.flush(150).is_empty());
        let emitted = tracker.flush(200);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].handle, 5);
        assert_eq!(emitted[0].commit_ts, 200);
    }

    #[test]
    fn test_tracker_prewrite_commit_pair() {
        let mut tracker = TransactionTracker::new();
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 300,
                commit_ts: 0,
                r#type: 1, // PREWRITE carries the value
                op_type: 1,
                key: record_key(9),
                value: vec![9, 9],
                ..Default::default()
            },
        );
        assert!(tracker.flush(u64::MAX).is_empty());
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 300,
                commit_ts: 301,
                r#type: 2, // COMMIT carries only key + ts
                op_type: 1,
                key: record_key(9),
                value: vec![],
                ..Default::default()
            },
        );
        let emitted = tracker.flush(301);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].handle, 9);
        // The value must come from the prewrite side.
        assert_eq!(emitted[0].value, vec![9, 9]);
    }

    #[test]
    fn test_tracker_caches_commit_before_initialized_and_replays() {
        let mut tracker = TransactionTracker::new();
        // COMMIT before the region's INITIALIZED marker, prewrite unseen.
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 10,
                commit_ts: 11,
                r#type: 2,
                op_type: 1,
                key: record_key(3),
                ..Default::default()
            },
        );
        assert!(tracker.flush(u64::MAX).is_empty());
        // The prewrite arrives late (scan-phase reordering), then the marker.
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 10,
                r#type: 1,
                op_type: 1,
                key: record_key(3),
                value: vec![7],
                ..Default::default()
            },
        );
        tracker.on_row(
            7,
            &CdcRow {
                r#type: 5,
                ..Default::default()
            },
        );
        let emitted = tracker.flush(11);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].handle, 3);
        assert_eq!(emitted[0].value, vec![7]);
    }

    #[test]
    fn test_tracker_fake_prewrite_does_not_overwrite_real_one() {
        let mut tracker = TransactionTracker::new();
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 50,
                r#type: 1,
                op_type: 1,
                key: record_key(4),
                value: vec![1, 2, 3, 4],
                ..Default::default()
            },
        );
        // Fake heartbeat prewrite (empty value) must not replace the real one.
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 50,
                r#type: 1,
                op_type: 1,
                key: record_key(4),
                value: vec![],
                ..Default::default()
            },
        );
        tracker.on_row(
            7,
            &CdcRow {
                start_ts: 50,
                commit_ts: 51,
                r#type: 2,
                op_type: 1,
                key: record_key(4),
                ..Default::default()
            },
        );
        let emitted = tracker.flush(51);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].value, vec![1, 2, 3, 4]);
    }
}
