/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Node log file viewer: lists and tails the engine's daily rolling log
//! files (`master.YYYY-MM-DD`, `worker.YYYY-MM-DD`, ...) for the console's
//! Logs page. Filenames are strictly whitelisted against the rolling
//! appender's `<role>.<date>.log` shape, so the endpoint can never escape
//! the configured log directory.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::dto::ErrorDto;
use crate::AppState;

/// A daily rolling file name, e.g. `master.2026-09-02.log` / `hybrid.2026-09-02.log`.
fn is_log_file_name(name: &str) -> bool {
    let Some((role, rest)) = name.split_once('.') else {
        return false;
    };
    if !matches!(role, "master" | "worker" | "hybrid") {
        return false;
    }
    // rest = YYYY-MM-DD.log
    let Some((date, "log")) = rest.rsplit_once('.') else {
        return false;
    };
    let bytes = date.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes.iter().enumerate().all(|(i, b)| {
            if i == 4 || i == 7 {
                true
            } else {
                b.is_ascii_digit()
            }
        })
}

#[derive(Debug, Serialize)]
pub struct LogFileListDto {
    /// File names, newest last.
    pub files: Vec<String>,
    /// Present (with a hint) when the node log directory is not exposed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LogQuery {
    /// How many trailing lines to return (default 500, cap 20_000).
    pub tail: Option<usize>,
    /// Substring filter applied after the level filter.
    pub q: Option<String>,
    /// Comma/space separated level names to keep (e.g. "ERROR,WARN").
    /// Case-insensitive; empty or absent = no level filter.
    pub level: Option<String>,
    /// `download=1` returns the raw filtered text as an attachment
    /// instead of JSON.
    pub download: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LogContentDto {
    pub name: String,
    /// True when the file was larger than the read cap and only its tail
    /// is included.
    pub truncated: bool,
    /// Filtered trailing lines, oldest first.
    pub lines: Vec<String>,
}

/// Hard cap on bytes read from one file per request (tail is taken from
/// the end) so a huge log cannot blow up the console process.
const MAX_READ_BYTES: u64 = 8 * 1024 * 1024;

fn log_dir(state: &AppState) -> Option<std::path::PathBuf> {
    state
        .log_dir
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .map(std::path::PathBuf::from)
}

/// `GET /api/v1/logs/files` — list the node's rolling log files.
pub async fn log_files(State(state): State<AppState>) -> Response {
    let Some(dir) = log_dir(&state) else {
        return (
            StatusCode::NOT_FOUND,
            Json(LogFileListDto {
                files: Vec::new(),
                error: Some(
                    "no log directory configured for the console (start with --log-dir or use \
                     the embedded --web console)"
                        .to_string(),
                ),
            }),
        )
            .into_response();
    };
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().to_string())
                .filter(|name| is_log_file_name(name))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    Json(LogFileListDto {
        files,
        error: None,
    })
    .into_response()
}

/// `GET /api/v1/logs/files/{name}?tail=500&q=&level=ERROR,WARN` — the
/// filtered tail of one log file.
pub async fn log_file(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<LogQuery>,
) -> Response {
    let Some(dir) = log_dir(&state) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorDto {
                error: "no log directory configured for the console".to_string(),
            }),
        )
            .into_response();
    };
    if !is_log_file_name(&name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorDto {
                error: "invalid log file name".to_string(),
            }),
        )
            .into_response();
    }
    let path = dir.join(&name);
    let bytes = match std::fs::metadata(&path) {
        Ok(meta) => {
            let size = meta.len();
            let skip = size.saturating_sub(MAX_READ_BYTES);
            read_tail(&path, skip)
        }
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "log file not found",
        )),
    };
    let (raw, truncated) = match bytes {
        Ok((raw, truncated)) => (raw, truncated),
        Err(err) => {
            let (status, message) = if err.kind() == std::io::ErrorKind::NotFound {
                (StatusCode::NOT_FOUND, format!("log file {} not found", name))
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("cannot read log file {}: {}", name, err),
                )
            };
            return (status, Json(ErrorDto { error: message })).into_response();
        }
    };

    let tail = query.tail.unwrap_or(500).clamp(1, 20_000);
    let needle = query.q.clone().unwrap_or_default().to_lowercase();
    let levels: Vec<String> = query
        .level
        .clone()
        .unwrap_or_default()
        .split([',', ' '])
        .filter_map(|l| {
            let l = l.trim().to_ascii_uppercase();
            (!l.is_empty()).then_some(l)
        })
        .collect();

    let all_lines: Vec<&str> = raw.split_inclusive('\n').collect();
    let filtered: Vec<String> = all_lines
        .iter()
        .filter(|line| {
            levels.is_empty()
                || levels
                    .iter()
                    .any(|level| line.contains(&format!(" {level} ")))
        })
        .filter(|line| needle.is_empty() || line.to_lowercase().contains(&needle))
        .map(|line| line.trim_end_matches('\n').to_string())
        .collect();
    let lines: Vec<String> = filtered
        .iter()
        .skip(filtered.len().saturating_sub(tail))
        .cloned()
        .collect();

    // Raw text download of the (filtered) tail.
    if query.download.as_deref() == Some("1") {
        let body = lines.join("\n");
        return (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", name),
            )],
            body,
        )
            .into_response();
    }

    Json(LogContentDto {
        name,
        truncated,
        lines,
    })
    .into_response()
}

/// Read from `skip` bytes to EOF; `truncated` reports whether anything was
/// skipped. The skipped region is cut at the first newline so the tail
/// starts at a complete line.
fn read_tail(path: &std::path::Path, skip: u64) -> std::io::Result<(String, bool)> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let skip = skip.min(size);
    file.seek(SeekFrom::Start(skip))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let truncated = skip > 0;
    let raw = if truncated {
        // Drop the partial first line.
        match buf.find('\n') {
            Some(pos) => buf[pos + 1..].to_string(),
            None => String::new(),
        }
    } else {
        buf
    };
    Ok((raw, truncated))
}

#[cfg(test)]
mod tests {
    use super::is_log_file_name;

    #[test]
    fn whitelist_rejects_traversal_and_garbage() {
        assert!(is_log_file_name("master.2026-09-02.log"));
        assert!(is_log_file_name("hybrid.2026-01-01.log"));
        assert!(is_log_file_name("worker.2026-12-31.log"));
        // Calendar validity is not checked — only the file-name shape.
        assert!(is_log_file_name("master.2026-13-99.log"));
        assert!(!is_log_file_name("../../../etc/passwd"));
        assert!(!is_log_file_name(".."));
        assert!(!is_log_file_name("raft"));
        assert!(!is_log_file_name("state.json"));
        assert!(!is_log_file_name("evil.2026-09-02.log")); // unknown role
        assert!(!is_log_file_name("master.2026-09-02.log.bak"));
    }
}
