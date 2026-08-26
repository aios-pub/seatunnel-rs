/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Simple HOCON (Human-Optimized Config Object Notation) parser.
//! Supports nested braces and dot-notation keys.
//! Produces a nested JSON Value tree.

use serde_json::Map;
use serde_json::Value;

/// Type alias for JSON object maps in the parser.
type JsonMap = Map<String, Value>;

/// Token types for the HOCON lexer.
#[derive(Debug, Clone)]
enum Token {
    Ident(String),
    LBrace,
    RBrace,
    Equals,
    StringLit(String),
    NumberLit(String),
    BoolLit(bool),
    Eof,
}
/// Lex HOCON input into tokens.
fn lex(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\r' | '\n' => {
                chars.next();
            }
            '#' => {
                while let Some(&ch) = chars.peek() {
                    if ch == '\n' {
                        break;
                    }
                    chars.next();
                }
            }
            '{' => {
                chars.next();
                tokens.push(Token::LBrace);
            }
            '}' => {
                chars.next();
                tokens.push(Token::RBrace);
            }
            '=' => {
                chars.next();
                tokens.push(Token::Equals);
            }
            '"' => {
                chars.next();
                let mut s = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == '"' {
                        chars.next();
                        break;
                    }
                    s.push(ch);
                    chars.next();
                }
                tokens.push(Token::StringLit(s));
            }
            '.' => {
                chars.next();
                tokens.push(Token::Ident(".".to_string()));
            }
            '-' | '0'..='9' => {
                let mut num = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_ascii_digit() || ch == '-' {
                        num.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::NumberLit(num));
            }
            't' | 'T' => {
                chars.next();
                if peek_matches(&mut chars, "rue") {
                    tokens.push(Token::BoolLit(true));
                } else {
                    let mut ident = String::new();
                    ident.push(c);
                    while let Some(&ch) = chars.peek() {
                        if ch.is_alphanumeric() || ch == '-' {
                            ident.push(ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    tokens.push(Token::Ident(ident));
                }
            }
            'f' | 'F' => {
                chars.next();
                if peek_matches(&mut chars, "alse") {
                    tokens.push(Token::BoolLit(false));
                } else {
                    let mut ident = String::new();
                    ident.push(c);
                    while let Some(&ch) = chars.peek() {
                        if ch.is_alphanumeric() || ch == '-' {
                            ident.push(ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    tokens.push(Token::Ident(ident));
                }
            }
            _ => {
                let mut ident = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_alphanumeric() || ch == '-' {
                        ident.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if !ident.is_empty() {
                    tokens.push(Token::Ident(ident));
                }
            }
        }
    }
    tokens.push(Token::Eof);
    tokens
}
fn peek_matches(chars: &mut std::iter::Peekable<std::str::Chars>, expected: &str) -> bool {
    let remaining: String = chars.clone().collect();
    if remaining.starts_with(expected) {
        for _ in 0..expected.len() {
            chars.next();
        }
        true
    } else {
        false
    }
}
/// Parse dot-notation key path into vector of segments.
fn parse_dot_key(tokens: &[Token], pos: usize) -> (Vec<String>, usize) {
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut p = pos;

    loop {
        if p >= tokens.len() {
            break;
        }
        match &tokens[p] {
            Token::Ident(s) if s == "." => {
                if !buf.is_empty() {
                    parts.push(buf.clone());
                    buf.clear();
                }
                p += 1;
            }
            Token::Ident(s) => {
                if !buf.is_empty() {
                    parts.push(buf.clone());
                    buf.clear();
                }
                buf = s.clone();
                p += 1;
            }
            _ => break,
        }
    }
    if !buf.is_empty() {
        parts.push(buf);
    }
    (parts, p)
}
/// Set a value at a dot-notation path in a JSON map, creating intermediate objects.
fn set_dot_path(map: &mut JsonMap, path: &[String], value: Value) {
    if path.is_empty() {
        return;
    }
    let last_idx = path.len() - 1;
    let mut idx = 0;
    let mut current_raw = map as *mut JsonMap;
    while idx < path.len() {
        unsafe {
            let current = &mut *current_raw;
            let seg = path[idx].clone();
            if idx == last_idx {
                current.insert(seg, value);
                return;
            }
            let entry = current
                .entry(seg)
                .or_insert_with(|| Value::Object(JsonMap::new()));
            let val_ptr = entry as *mut Value;
            if let Some(obj) = (*val_ptr).as_object_mut() {
                current_raw = obj as *mut JsonMap;
            } else {
                return;
            }
        }
        idx += 1;
    }
}
/// Get a value from a dot-notation path in a JSON value.
pub fn get_dot_path<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for seg in path {
        current = current.get(seg)?;
    }
    Some(current)
}
/// Parse HOCON content into a JSON Value.
pub fn parse_hocon(content: &str) -> Result<Value, String> {
    let tokens = lex(content);
    let mut root: JsonMap = JsonMap::new();
    parse_block(&tokens, 0, &mut root).map(|_| Value::Object(root))
}
fn parse_block(
    tokens: &[Token],
    pos: usize,
    current: &mut JsonMap,
) -> Result<(usize, usize), String> {
    let mut p = pos;

    while p < tokens.len() {
        match &tokens[p] {
            Token::RBrace | Token::Eof => break,
            Token::Ident(_) => {
                let (key_parts, next_p) = parse_dot_key(tokens, p);
                if key_parts.is_empty() {
                    p += 1;
                    continue;
                }
                if next_p >= tokens.len() {
                    break;
                }
                match &tokens[next_p] {
                    Token::LBrace => {
                        let mut sub_map: JsonMap = JsonMap::new();
                        let (inner_end, _) = parse_block(tokens, next_p + 1, &mut sub_map)?;
                        let val = Value::Object(sub_map);
                        set_dot_path(current, &key_parts, val);
                        if inner_end < tokens.len() && matches!(&tokens[inner_end], Token::RBrace) {
                            p = inner_end + 1;
                        } else {
                            p = inner_end;
                        }
                    }
                    Token::Equals => {
                        let val_p = next_p + 1;
                        let value = if val_p < tokens.len() {
                            match &tokens[val_p] {
                                Token::StringLit(s) => Value::String(s.clone()),
                                Token::NumberLit(n) => n
                                    .parse::<i64>()
                                    .ok()
                                    .map(Value::from)
                                    .unwrap_or(Value::String(n.clone())),
                                Token::BoolLit(b) => Value::Bool(*b),
                                _ => Value::Null,
                            }
                        } else {
                            Value::Null
                        };
                        set_dot_path(current, &key_parts, value);
                        p = val_p + 1;
                    }
                    _ => {
                        p = next_p;
                    }
                }
            }
            _ => {
                p += 1;
            }
        }
    }
    Ok((p, 0))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_env_section() {
        let input = r#"env { job.name = "demo" job.mode = "streaming" parallelism.default = 4 }"#;
        let result = parse_hocon(input).unwrap();
        assert_eq!(result["env"]["job"]["name"], "demo");
        assert_eq!(result["env"]["parallelism"]["default"], 4);
    }
    #[test]
    fn test_parse_source_section() {
        let input = r#"source { kafka { bootstrap.servers = "localhost:9092" topic = "my-topic" format = "json" } }"#;
        let result = parse_hocon(input).unwrap();
        assert_eq!(result["source"]["kafka"]["topic"], "my-topic");
        assert_eq!(
            result["source"]["kafka"]["bootstrap"]["servers"],
            "localhost:9092"
        );
    }
    #[test]
    fn test_parse_full_config() {
        let input = r#"
            env {
              job.name = "demo"
              job.mode = "streaming"
              parallelism.default = 4
            }
            source {
              kafka {
                bootstrap.servers = "localhost:9092"
                topic = "test"
                format = "json"
              }
            }
            sink {
              console {
                format = "json"
              }
            }
        "#;
        let result = parse_hocon(input).unwrap();
        assert_eq!(result["env"]["job"]["name"], "demo");
        assert_eq!(result["source"]["kafka"]["topic"], "test");
        assert_eq!(result["sink"]["console"]["format"], "json");
    }
    #[test]
    fn test_parse_bool() {
        let input = r#"env { checkpoint.enabled = true }"#;
        let result = parse_hocon(input).unwrap();
        assert_eq!(result["env"]["checkpoint"]["enabled"], true);
    }
    #[test]
    fn test_parse_comment() {
        let input = r#"env { # this is a comment
          job.name = "demo"
        }"#;
        let result = parse_hocon(input).unwrap();
        assert_eq!(result["env"]["job"]["name"], "demo");
    }
}
