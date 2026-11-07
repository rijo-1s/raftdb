//! Minimal JSON value, writer and parser (no external dependency).

use std::fmt::Write;

#[derive(Debug, Clone, PartialEq)]
pub enum J {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

pub fn obj(items: Vec<(&str, J)>) -> J {
    J::Obj(items.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}
pub fn s(v: impl Into<String>) -> J {
    J::Str(v.into())
}
pub fn n(v: impl Into<f64>) -> J {
    J::Num(v.into())
}
pub fn nu(v: u64) -> J {
    J::Num(v as f64)
}

impl J {
    pub fn get(&self, k: &str) -> Option<&J> {
        match self {
            J::Obj(v) => v.iter().find(|(kk, _)| kk == k).map(|(_, v)| v),
            _ => None,
        }
    }
    pub fn num(&self) -> Option<f64> {
        match self {
            J::Num(x) => Some(*x),
            _ => None,
        }
    }
    pub fn str(&self) -> Option<&str> {
        match self {
            J::Str(x) => Some(x),
            _ => None,
        }
    }
    pub fn arr(&self) -> Option<&[J]> {
        match self {
            J::Arr(x) => Some(x),
            _ => None,
        }
    }
    pub fn path(&self, p: &str) -> Option<&J> {
        let mut cur = self;
        for part in p.split('.') {
            cur = cur.get(part)?;
        }
        Some(cur)
    }

    pub fn pretty(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out.push('\n');
        out
    }

    fn write(&self, out: &mut String, ind: usize) {
        match self {
            J::Null => out.push_str("null"),
            J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            J::Num(x) => {
                if !x.is_finite() {
                    out.push_str("null")
                } else if x.fract() == 0.0 && x.abs() < 1e15 {
                    let _ = write!(out, "{}", *x as i64);
                } else {
                    let _ = write!(out, "{:.6}", x);
                }
            }
            J::Str(t) => {
                out.push('"');
                for c in t.chars() {
                    match c {
                        '"' => out.push_str("\\\""),
                        '\\' => out.push_str("\\\\"),
                        '\n' => out.push_str("\\n"),
                        '\r' => out.push_str("\\r"),
                        '\t' => out.push_str("\\t"),
                        c if (c as u32) < 0x20 => {
                            let _ = write!(out, "\\u{:04x}", c as u32);
                        }
                        c => out.push(c),
                    }
                }
                out.push('"');
            }
            J::Arr(v) => {
                if v.iter().all(|x| !matches!(x, J::Arr(_) | J::Obj(_))) {
                    out.push('[');
                    for (i, x) in v.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        x.write(out, ind);
                    }
                    out.push(']');
                } else {
                    out.push_str("[\n");
                    for (i, x) in v.iter().enumerate() {
                        out.push_str(&" ".repeat(ind + 2));
                        x.write(out, ind + 2);
                        if i + 1 < v.len() {
                            out.push(',');
                        }
                        out.push('\n');
                    }
                    out.push_str(&" ".repeat(ind));
                    out.push(']');
                }
            }
            J::Obj(v) => {
                out.push_str("{\n");
                for (i, (k, x)) in v.iter().enumerate() {
                    out.push_str(&" ".repeat(ind + 2));
                    J::Str(k.clone()).write(out, 0);
                    out.push_str(": ");
                    x.write(out, ind + 2);
                    if i + 1 < v.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str(&" ".repeat(ind));
                out.push('}');
            }
        }
    }

    pub fn parse(src: &str) -> Result<J, String> {
        let b = src.as_bytes();
        let mut i = 0;
        let v = parse_val(b, &mut i)?;
        skip_ws(b, &mut i);
        if i != b.len() {
            return Err(format!("trailing data at byte {i}"));
        }
        Ok(v)
    }
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && (b[*i] as char).is_ascii_whitespace() {
        *i += 1;
    }
}

fn parse_val(b: &[u8], i: &mut usize) -> Result<J, String> {
    skip_ws(b, i);
    let c = *b.get(*i).ok_or("unexpected end")?;
    match c {
        b'n' if b[*i..].starts_with(b"null") => {
            *i += 4;
            Ok(J::Null)
        }
        b't' if b[*i..].starts_with(b"true") => {
            *i += 4;
            Ok(J::Bool(true))
        }
        b'f' if b[*i..].starts_with(b"false") => {
            *i += 5;
            Ok(J::Bool(false))
        }
        b'"' => Ok(J::Str(parse_str(b, i)?)),
        b'[' => {
            *i += 1;
            let mut v = Vec::new();
            loop {
                skip_ws(b, i);
                if b.get(*i) == Some(&b']') {
                    *i += 1;
                    return Ok(J::Arr(v));
                }
                v.push(parse_val(b, i)?);
                skip_ws(b, i);
                match b.get(*i) {
                    Some(b',') => *i += 1,
                    Some(b']') => {}
                    _ => return Err(format!("bad array at byte {i}")),
                }
            }
        }
        b'{' => {
            *i += 1;
            let mut v = Vec::new();
            loop {
                skip_ws(b, i);
                if b.get(*i) == Some(&b'}') {
                    *i += 1;
                    return Ok(J::Obj(v));
                }
                let k = parse_str(b, i)?;
                skip_ws(b, i);
                if b.get(*i) != Some(&b':') {
                    return Err(format!("expected ':' at byte {i}"));
                }
                *i += 1;
                v.push((k, parse_val(b, i)?));
                skip_ws(b, i);
                match b.get(*i) {
                    Some(b',') => *i += 1,
                    Some(b'}') => {}
                    _ => return Err(format!("bad object at byte {i}")),
                }
            }
        }
        _ => {
            let st = *i;
            while *i < b.len() && matches!(b[*i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
                *i += 1;
            }
            std::str::from_utf8(&b[st..*i])
                .ok()
                .and_then(|t| t.parse::<f64>().ok())
                .map(J::Num)
                .ok_or_else(|| format!("bad number at byte {st}"))
        }
    }
}

fn parse_str(b: &[u8], i: &mut usize) -> Result<String, String> {
    if b.get(*i) != Some(&b'"') {
        return Err(format!("expected string at byte {i}"));
    }
    *i += 1;
    let mut out: Vec<u8> = Vec::new();
    loop {
        let c = *b.get(*i).ok_or("unterminated string")?;
        *i += 1;
        match c {
            b'"' => return String::from_utf8(out).map_err(|e| e.to_string()),
            b'\\' => {
                let e = *b.get(*i).ok_or("bad escape")?;
                *i += 1;
                match e {
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'u' => {
                        let h = std::str::from_utf8(b.get(*i..*i + 4).ok_or("bad \\u")?)
                            .map_err(|e| e.to_string())?;
                        let cp = u32::from_str_radix(h, 16).map_err(|e| e.to_string())?;
                        *i += 4;
                        let ch = char::from_u32(cp).unwrap_or('?');
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    }
                    other => out.push(other),
                }
            }
            c => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let v = obj(vec![
            ("a", n(1.5)),
            ("b", J::Arr(vec![nu(1), nu(2)])),
            ("c", s("x\"y\n")),
            ("d", obj(vec![("e", J::Bool(true)), ("f", J::Null)])),
        ]);
        let t = v.pretty();
        assert_eq!(J::parse(&t).unwrap(), v);
    }
}
