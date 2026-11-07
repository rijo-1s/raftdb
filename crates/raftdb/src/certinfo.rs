//! When does the node's certificate expire?
//!
//! Only the `notAfter` date is needed, and it sits at a fixed place in the
//! certificate's DER encoding, so it is read directly instead of pulling in a
//! full X.509 parser. Every length is bounds-checked: arbitrary bytes give an
//! error, never a panic.

use std::time::{SystemTime, UNIX_EPOCH};

/// Walks DER type-length-value items.
struct Der<'a>(&'a [u8]);

impl<'a> Der<'a> {
    fn next(&mut self) -> Result<(u8, &'a [u8]), String> {
        let b = self.0;
        if b.len() < 2 {
            return Err("truncated certificate".into());
        }
        let (tag, first) = (b[0], b[1]);
        let (len, header) = if first < 0x80 {
            (first as usize, 2)
        } else {
            let n = (first & 0x7f) as usize;
            if n == 0 || n > 4 || b.len() < 2 + n {
                return Err("bad length in certificate".into());
            }
            (
                b[2..2 + n]
                    .iter()
                    .fold(0usize, |l, &x| (l << 8) | x as usize),
                2 + n,
            )
        };
        let end = header
            .checked_add(len)
            .filter(|&e| e <= b.len())
            .ok_or("truncated certificate")?;
        self.0 = &b[end..];
        Ok((tag, &b[header..end]))
    }
}

fn expect(tag: u8, want: u8, what: &str) -> Result<(), String> {
    if tag == want {
        Ok(())
    } else {
        Err(format!("unexpected certificate structure at {what}"))
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `UTCTime` (`YYMMDDHHMMSSZ`) or `GeneralizedTime` (`YYYYMMDDHHMMSSZ`).
fn parse_time(tag: u8, v: &[u8]) -> Result<i64, String> {
    let digits = |s: &[u8]| -> Result<i64, String> {
        s.iter().try_fold(0i64, |n, &c| {
            if c.is_ascii_digit() {
                Ok(n * 10 + i64::from(c - b'0'))
            } else {
                Err("bad date in certificate".to_string())
            }
        })
    };
    let (year, rest) = match (tag, v.len()) {
        (0x17, 13) => {
            let yy = digits(&v[..2])?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, &v[2..])
        }
        (0x18, 15) => (digits(&v[..4])?, &v[4..]),
        _ => return Err("unsupported date format in certificate".into()),
    };
    if rest[10] != b'Z' {
        return Err("certificate date is not UTC".into());
    }
    let (mo, d, h, mi, s) = (
        digits(&rest[0..2])?,
        digits(&rest[2..4])?,
        digits(&rest[4..6])?,
        digits(&rest[6..8])?,
        digits(&rest[8..10])?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || s > 60 {
        return Err("bad date in certificate".into());
    }
    Ok(days_from_civil(year, mo, d) * 86_400 + h * 3_600 + mi * 60 + s)
}

/// The `notAfter` time of a DER certificate, as Unix seconds.
pub fn not_after_unix(der: &[u8]) -> Result<i64, String> {
    let (t, cert) = Der(der).next()?;
    expect(t, 0x30, "certificate")?;
    let (t, tbs) = Der(cert).next()?;
    expect(t, 0x30, "tbsCertificate")?;
    let mut f = Der(tbs);
    let (mut tag, _) = f.next()?; // optional [0] version, else the serial
    if tag == 0xA0 {
        tag = f.next()?.0;
    }
    expect(tag, 0x02, "serial number")?;
    for what in ["signature algorithm", "issuer"] {
        expect(f.next()?.0, 0x30, what)?;
    }
    let (t, validity) = f.next()?;
    expect(t, 0x30, "validity")?;
    let mut v = Der(validity);
    v.next()?; // notBefore
    let (t, not_after) = v.next()?;
    parse_time(t, not_after)
}

/// `notAfter` of the first certificate (the node's own) in a PEM file.
pub fn leaf_not_after_from_pem(pem: &[u8]) -> Result<i64, String> {
    let first = rustls_pemfile::certs(&mut &*pem)
        .next()
        .ok_or("no certificate found in the PEM file")?
        .map_err(|e| format!("unreadable PEM: {e}"))?;
    not_after_unix(first.as_ref())
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert_expiring(y: i32, m: u8, d: u8) -> rcgen::Certificate {
        let mut p = rcgen::CertificateParams::new(vec!["localhost".to_string()]);
        p.not_after = rcgen::date_time_ymd(y, m, d);
        rcgen::Certificate::from_params(p).unwrap()
    }

    #[test]
    fn calendar_arithmetic_matches_known_dates() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(2024, 2, 29), 19_782, "a leap day");
        assert_eq!(parse_time(0x18, b"20300101000000Z"), Ok(1_893_456_000));
        assert_eq!(parse_time(0x17, b"300101000000Z"), Ok(1_893_456_000));
        assert_eq!(
            parse_time(0x17, b"700101000000Z"),
            Ok(0),
            "YY >= 50 is 19YY"
        );
    }

    #[test]
    fn the_expiry_of_a_real_certificate_is_read_back_exactly() {
        for (y, m, d, unix) in [
            (2030, 1, 1, 1893456000i64),
            (2049, 12, 31, 2524521600), // UTCTime range
            (2051, 1, 1, 2556144000),   // beyond UTCTime: GeneralizedTime
        ] {
            let c = cert_expiring(y, m, d);
            assert_eq!(
                not_after_unix(&c.serialize_der().unwrap()),
                Ok(unix),
                "{y}-{m}-{d}"
            );
        }
    }

    #[test]
    fn a_pem_bundle_reports_the_first_certificate() {
        let leaf = cert_expiring(2030, 1, 1);
        let other = cert_expiring(2040, 1, 1);
        let pem = format!(
            "{}{}",
            leaf.serialize_pem().unwrap(),
            other.serialize_pem().unwrap()
        );
        assert_eq!(leaf_not_after_from_pem(pem.as_bytes()), Ok(1_893_456_000));
        assert!(leaf_not_after_from_pem(b"not a certificate").is_err());
        assert!(leaf_not_after_from_pem(b"").is_err());
    }

    #[test]
    fn damaged_certificates_are_errors_never_panics() {
        let der = cert_expiring(2030, 1, 1).serialize_der().unwrap();
        for cut in 0..der.len() {
            let _ = not_after_unix(&der[..cut]);
        }
        for i in 0..der.len() {
            for mask in [0x01u8, 0x80, 0xFF] {
                let mut d = der.clone();
                d[i] ^= mask;
                let _ = not_after_unix(&d);
            }
        }
        assert!(not_after_unix(&[]).is_err());
        assert!(not_after_unix(&[0x30, 0x84, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }
}
