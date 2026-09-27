//! Dependency-free ingestion for the portable V0 interchange format.
//!
//! This crate deliberately stops at validated input records. Resolution and
//! persistence belong to `wm-resolution`, so callers can inspect or reject a
//! complete batch before changing the database.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Format {
    Json,
    JsonLines,
    Csv,
}

impl Format {
    pub fn from_path(path: &Path) -> Result<Self, ParseError> {
        match path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase()
            .as_str()
        {
            "json" => Ok(Self::Json),
            "jsonl" | "ndjson" => Ok(Self::JsonLines),
            "csv" => Ok(Self::Csv),
            other => Err(ParseError::new(format!(
                "unsupported input extension: {other}"
            ))),
        }
    }
}

/// A transport-level observation. Values are strings so ingestion stays
/// decoupled from the core model and can preserve the original spelling.
#[derive(Clone, Debug, PartialEq)]
pub struct InputRecord {
    pub source: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub observed_at: String,
    pub confidence: f64,
    pub raw_payload: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseError {}

pub fn read(mut reader: impl Read, format: Format) -> Result<Vec<InputRecord>, ParseError> {
    let mut text = String::new();
    reader
        .read_to_string(&mut text)
        .map_err(|e| ParseError::new(format!("read input: {e}")))?;
    parse(&text, format)
}

pub fn parse(text: &str, format: Format) -> Result<Vec<InputRecord>, ParseError> {
    match format {
        Format::Json => parse_json_document(text),
        Format::JsonLines => text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty())
            .map(|(index, line)| {
                parse_json_object(line)
                    .and_then(map_to_record)
                    .map_err(|e| ParseError::new(format!("JSONL line {}: {e}", index + 1)))
            })
            .collect(),
        Format::Csv => parse_csv(text),
    }
}

/// Parse a flat JSON object for transport adapters. Nested values are retained
/// as their raw JSON text.
pub fn parse_object(text: &str) -> Result<BTreeMap<String, String>, ParseError> {
    parse_json_object(text)
}

fn parse_json_document(text: &str) -> Result<Vec<InputRecord>, ParseError> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        return Ok(vec![map_to_record(parse_json_object(trimmed)?)?]);
    }
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(ParseError::new(
            "JSON input must be an object or array of objects",
        ));
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    split_top_level(inner, ',')?
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .enumerate()
        .map(|(index, part)| {
            parse_json_object(part)
                .and_then(map_to_record)
                .map_err(|e| ParseError::new(format!("JSON item {}: {e}", index + 1)))
        })
        .collect()
}

fn parse_json_object(text: &str) -> Result<BTreeMap<String, String>, ParseError> {
    let text = text.trim();
    if !text.starts_with('{') || !text.ends_with('}') {
        return Err(ParseError::new("expected JSON object"));
    }
    let mut output = BTreeMap::new();
    for pair in split_top_level(&text[1..text.len() - 1], ',')? {
        if pair.trim().is_empty() {
            continue;
        }
        let pieces = split_top_level(pair, ':')?;
        if pieces.len() != 2 {
            return Err(ParseError::new(format!("invalid object member: {pair}")));
        }
        let key = decode_json_string(pieces[0].trim())?;
        let raw = pieces[1].trim();
        let value = if raw.starts_with('"') {
            decode_json_string(raw)?
        } else if raw == "null" {
            String::new()
        } else {
            raw.to_string()
        };
        output.insert(key, value);
    }
    Ok(output)
}

/// Split only at depth zero, respecting JSON strings and escape sequences.
fn split_top_level(text: &str, separator: char) -> Result<Vec<&str>, ParseError> {
    let mut result = Vec::new();
    let (mut start, mut depth, mut in_string, mut escaped) = (0, 0_i32, false, false);
    for (index, ch) in text.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' | '[' => depth += 1,
            '}' | ']' => {
                depth -= 1;
                if depth < 0 {
                    return Err(ParseError::new("unbalanced JSON delimiters"));
                }
            }
            _ if ch == separator && depth == 0 => {
                result.push(&text[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_string || depth != 0 {
        return Err(ParseError::new("unterminated JSON value"));
    }
    result.push(&text[start..]);
    Ok(result)
}

fn decode_json_string(value: &str) -> Result<String, ParseError> {
    if !value.starts_with('"') || !value.ends_with('"') {
        return Err(ParseError::new(
            "JSON keys and string values must be quoted",
        ));
    }
    let mut result = String::new();
    let mut chars = value[1..value.len() - 1].chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            result.push(ch);
            continue;
        }
        match chars
            .next()
            .ok_or_else(|| ParseError::new("unterminated JSON escape"))?
        {
            '"' => result.push('"'),
            '\\' => result.push('\\'),
            '/' => result.push('/'),
            'b' => result.push('\u{8}'),
            'f' => result.push('\u{c}'),
            'n' => result.push('\n'),
            'r' => result.push('\r'),
            't' => result.push('\t'),
            'u' => {
                let digits: String = chars.by_ref().take(4).collect();
                if digits.len() != 4 {
                    return Err(ParseError::new("short unicode escape"));
                }
                let code = u32::from_str_radix(&digits, 16)
                    .map_err(|_| ParseError::new("invalid unicode escape"))?;
                result.push(
                    char::from_u32(code)
                        .ok_or_else(|| ParseError::new("invalid unicode scalar"))?,
                );
            }
            other => {
                return Err(ParseError::new(format!(
                    "unsupported JSON escape: \\{other}"
                )));
            }
        }
    }
    Ok(result)
}

fn map_to_record(mut values: BTreeMap<String, String>) -> Result<InputRecord, ParseError> {
    let required = |map: &mut BTreeMap<String, String>, key: &str| {
        map.remove(key)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| ParseError::new(format!("missing required field '{key}'")))
    };
    let source = required(&mut values, "source")?;
    let subject = required(&mut values, "subject")?;
    let predicate = required(&mut values, "predicate")?;
    let object = required(&mut values, "object")?;
    let observed_at = required(&mut values, "observed_at")?;
    let confidence = values
        .remove("confidence")
        .unwrap_or_else(|| "1.0".into())
        .parse::<f64>()
        .map_err(|_| ParseError::new("confidence must be a number"))?;
    if !(0.0..=1.0).contains(&confidence) {
        return Err(ParseError::new("confidence must be between 0 and 1"));
    }
    let raw_payload = values.remove("raw_payload").filter(|v| !v.is_empty());
    Ok(InputRecord {
        source,
        subject,
        predicate,
        object,
        observed_at,
        confidence,
        raw_payload,
        metadata: values,
    })
}

fn parse_csv(text: &str) -> Result<Vec<InputRecord>, ParseError> {
    let rows = csv_rows(text)?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let headers = &rows[0];
    let mut result = Vec::new();
    for (index, row) in rows.iter().enumerate().skip(1) {
        if row.iter().all(|v| v.trim().is_empty()) {
            continue;
        }
        if row.len() != headers.len() {
            return Err(ParseError::new(format!(
                "CSV row {} has {} columns; expected {}",
                index + 1,
                row.len(),
                headers.len()
            )));
        }
        let values = headers.iter().cloned().zip(row.iter().cloned()).collect();
        result.push(
            map_to_record(values)
                .map_err(|e| ParseError::new(format!("CSV row {}: {e}", index + 1)))?,
        );
    }
    Ok(result)
}

fn csv_rows(text: &str) -> Result<Vec<Vec<String>>, ParseError> {
    let mut rows = vec![vec![String::new()]];
    let (mut row, mut col, mut chars, mut quoted) =
        (0_usize, 0_usize, text.chars().peekable(), false);
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                rows[row][col].push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                rows[row].push(String::new());
                col += 1;
            }
            '\n' if !quoted => {
                rows.push(vec![String::new()]);
                row += 1;
                col = 0;
            }
            '\r' if !quoted && chars.peek() == Some(&'\n') => {}
            other => rows[row][col].push(other),
        }
    }
    if quoted {
        return Err(ParseError::new("unterminated quoted CSV field"));
    }
    if rows.last().is_some_and(|r| r.len() == 1 && r[0].is_empty()) {
        rows.pop();
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    const JSON: &str = r#"{"source":"src:filing","subject":"company:acme","predicate":"CEO","object":"person:alice","observed_at":"2026-09-01T12:00:00Z","confidence":0.98}"#;

    #[test]
    fn parses_json_and_jsonl() {
        let item = &parse(JSON, Format::Json).unwrap()[0];
        assert_eq!(item.object, "person:alice");
        assert_eq!(item.confidence, 0.98);
        assert_eq!(
            parse(&format!("{JSON}\n{JSON}\n"), Format::JsonLines)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn parses_json_array_and_keeps_metadata() {
        let records = parse(&format!("[{JSON},{JSON}]"), Format::Json).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn parses_quoted_csv() {
        let csv = "source,subject,predicate,object,observed_at,confidence,note\r\nsrc:x,company:x,NAME,\"Acme, Inc.\",2026-01-01T00:00:00Z,0.7,\"quoted \"\"value\"\"\"\r\n";
        let records = parse(csv, Format::Csv).unwrap();
        assert_eq!(records[0].object, "Acme, Inc.");
        assert_eq!(records[0].metadata["note"], "quoted \"value\"");
    }

    #[test]
    fn rejects_invalid_confidence() {
        assert!(parse(&JSON.replace("0.98", "1.2"), Format::Json).is_err());
    }
}
