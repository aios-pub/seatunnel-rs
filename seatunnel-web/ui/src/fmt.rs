// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Display formatting helpers (time, duration, byte sizes).

use wasm_bindgen::JsValue;

/// Render an epoch-milliseconds timestamp as `YYYY-MM-DD HH:MM:SS` in the
/// browser's local timezone. Assembled from date components by hand so every
/// browser shows the same year-first layout regardless of its locale data
/// (toLocaleString would render en-GB as 04/09/2026).
pub fn fmt_time(ms: i64) -> String {
    if ms <= 0 {
        return "-".to_string();
    }
    let date = js_sys::Date::new(&JsValue::from_f64(ms as f64));
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        date.get_full_year(),
        date.get_month() + 1,
        date.get_date(),
        date.get_hours(),
        date.get_minutes(),
        date.get_seconds()
    )
}

/// Human-readable duration between two epoch-milliseconds timestamps.
/// An unset end timestamp means "still running" (duration until now).
pub fn fmt_duration(start_ms: i64, end_ms: i64) -> String {
    if start_ms <= 0 {
        return "-".to_string();
    }
    let end = if end_ms > 0 {
        end_ms
    } else {
        js_sys::Date::now() as i64
    };
    let total_secs = (end - start_ms) / 1000;
    if total_secs < 0 {
        return "-".to_string();
    }
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

/// Short duration for idle indicators: 850ms, 3.2s, 1m 05s, 2h 14m.
pub fn fmt_short_duration(ms: i64) -> String {
    if ms < 0 {
        return "-".to_string();
    }
    let secs = ms as f64 / 1000.0;
    if secs < 10.0 {
        format!("{:.1}s", secs)
    } else if secs < 60.0 {
        format!("{:.0}s", secs)
    } else {
        let total = secs as i64;
        let hours = total / 3600;
        let minutes = (total % 3600) / 60;
        let s = total % 60;
        if hours > 0 {
            format!("{}h {:02}m", hours, minutes)
        } else {
            format!("{}m {:02}s", minutes, s)
        }
    }
}

/// Human-readable byte size.
pub fn fmt_bytes(bytes: i64) -> String {
    let mut size = bytes as f64;
    for unit in ["B", "KB", "MB", "GB", "TB"] {
        if size < 1024.0 {
            return format!("{:.1} {}", size, unit);
        }
        size /= 1024.0;
    }
    format!("{:.1} PB", size)
}

/// Thousands separator for record counters.
pub fn fmt_count(value: i64) -> String {
    value
        .to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap_or_default()
        .join(",")
}
