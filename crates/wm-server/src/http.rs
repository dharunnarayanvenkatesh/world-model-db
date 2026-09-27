use std::collections::BTreeMap;
use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub body: String,
}

impl Request {
    pub fn read(stream: &mut TcpStream) -> io::Result<Self> {
        let mut reader = io::BufReader::new(stream);
        let mut first = String::new();
        reader.read_line(&mut first)?;
        let mut words = first.split_whitespace();
        let method = words.next().unwrap_or_default().to_owned();
        let target = words.next().unwrap_or_default();
        if method.is_empty() || target.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid HTTP request line",
            ));
        }
        let mut length = 0_usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            if line == "\r\n" || line == "\n" || line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length")
                })?;
            }
        }
        // V0 bodies are deliberately bounded to keep this small synchronous
        // server predictable in the absence of authentication and quotas.
        if length > 1_048_576 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request body exceeds 1 MiB",
            ));
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body)?;
        let body = String::from_utf8(body)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "body is not UTF-8"))?;
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        Ok(Self {
            method,
            path: percent_decode(path)?,
            query: parse_query(query)?,
            body,
        })
    }
}

pub struct Response {
    pub status: u16,
    pub body: String,
}

impl Response {
    pub fn json(status: u16, body: String) -> Self {
        Self { status, body }
    }
    pub fn error(status: u16, message: &str) -> Self {
        Self::json(
            status,
            format!("{{\"error\":\"{}\"}}", json_escape(message)),
        )
    }
    pub fn write(self, stream: &mut TcpStream) -> io::Result<()> {
        let reason = match self.status {
            200 => "OK",
            201 => "Created",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            409 => "Conflict",
            _ => "Internal Server Error",
        };
        let bytes = self.body.as_bytes();
        write!(
            stream,
            "HTTP/1.1 {} {}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.status,
            reason,
            bytes.len()
        )?;
        stream.write_all(bytes)
    }
}

pub fn json_escape(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn parse_query(query: &str) -> io::Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for pair in query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        result.insert(
            percent_decode(&key.replace('+', " "))?,
            percent_decode(&value.replace('+', " "))?,
        );
    }
    Ok(result)
}

fn percent_decode(text: &str) -> io::Result<String> {
    let bytes = text.as_bytes();
    let mut output = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "incomplete percent escape",
                ));
            }
            let digits = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|_| io::ErrorKind::InvalidData)?;
            output.push(u8::from_str_radix(digits, 16).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid percent escape")
            })?);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "decoded value is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_and_json_encoding() {
        let q = parse_query("from=company%3Aacme&name=Nova+Labs").unwrap();
        assert_eq!(q["from"], "company:acme");
        assert_eq!(q["name"], "Nova Labs");
        assert_eq!(json_escape("a\"b\n"), "a\\\"b\\n");
    }
}
