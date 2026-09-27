//! Dependency-free validation and predicates for canonical RFC 3339 timestamps.
//!
//! Interval functions use half-open bounds (`from <= at < to`) and lexical
//! comparison. Callers should therefore use one canonical representation (UTC
//! with fixed-width fields is recommended) when ordering instants.

use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidTimestamp {
    pub value: String,
}

impl fmt::Display for InvalidTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "`{}` is not a valid RFC 3339 timestamp",
            self.value
        )
    }
}

impl Error for InvalidTimestamp {}

pub fn validate_rfc3339(value: &str) -> Result<(), InvalidTimestamp> {
    if is_valid_rfc3339(value) {
        Ok(())
    } else {
        Err(InvalidTimestamp {
            value: value.to_owned(),
        })
    }
}

pub fn is_valid_rfc3339(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b'T' | b't'))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }

    let Some(year) = decimal(bytes, 0, 4) else {
        return false;
    };
    let Some(month) = decimal(bytes, 5, 7) else {
        return false;
    };
    let Some(day) = decimal(bytes, 8, 10) else {
        return false;
    };
    let Some(hour) = decimal(bytes, 11, 13) else {
        return false;
    };
    let Some(minute) = decimal(bytes, 14, 16) else {
        return false;
    };
    let Some(second) = decimal(bytes, 17, 19) else {
        return false;
    };

    if month == 0
        || month > 12
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return false;
    }

    let mut cursor = 19;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == fraction_start {
            return false;
        }
    }

    match bytes.get(cursor) {
        Some(b'Z' | b'z') => cursor + 1 == bytes.len(),
        Some(b'+' | b'-') => {
            bytes.len() == cursor + 6
                && bytes.get(cursor + 3) == Some(&b':')
                && decimal(bytes, cursor + 1, cursor + 3).is_some_and(|hour| hour <= 23)
                && decimal(bytes, cursor + 4, cursor + 6).is_some_and(|minute| minute <= 59)
        }
        _ => false,
    }
}

fn decimal(bytes: &[u8], from: usize, to: usize) -> Option<u32> {
    let digits = bytes.get(from..to)?;
    digits.iter().all(u8::is_ascii_digit).then(|| {
        digits
            .iter()
            .fold(0, |value, digit| value * 10 + u32::from(digit - b'0'))
    })
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

/// Returns whether `at` is in the half-open interval `[from, to)`.
pub fn interval_contains(from: &str, to: Option<&str>, at: &str) -> bool {
    from <= at && to.is_none_or(|exclusive_end| at < exclusive_end)
}

/// A synonym with the instant first, convenient for predicate-style call sites.
pub fn is_in_interval(at: &str, from: &str, to: Option<&str>) -> bool {
    interval_contains(from, to, at)
}

/// Returns whether two half-open intervals intersect.
pub fn intervals_overlap(
    left_from: &str,
    left_to: Option<&str>,
    right_from: &str,
    right_to: Option<&str>,
) -> bool {
    let left_is_nonempty = left_to.is_none_or(|left_end| left_from < left_end);
    let right_is_nonempty = right_to.is_none_or(|right_end| right_from < right_end);

    left_is_nonempty
        && right_is_nonempty
        && left_to.is_none_or(|left_end| right_from < left_end)
        && right_to.is_none_or(|right_end| left_from < right_end)
}

/// Returns whether both the valid-time and transaction-time intervals contain
/// their respective query instants.
pub fn bitemporal_contains(
    valid_from: &str,
    valid_to: Option<&str>,
    known_from: &str,
    known_to: Option<&str>,
    valid_at: &str,
    known_at: &str,
) -> bool {
    interval_contains(valid_from, valid_to, valid_at)
        && interval_contains(known_from, known_to, known_at)
}

/// Returns whether both dimensions of two bitemporal records overlap.
#[allow(clippy::too_many_arguments)]
pub fn bitemporal_overlaps(
    left_valid_from: &str,
    left_valid_to: Option<&str>,
    left_known_from: &str,
    left_known_to: Option<&str>,
    right_valid_from: &str,
    right_valid_to: Option<&str>,
    right_known_from: &str,
    right_known_to: Option<&str>,
) -> bool {
    intervals_overlap(
        left_valid_from,
        left_valid_to,
        right_valid_from,
        right_valid_to,
    ) && intervals_overlap(
        left_known_from,
        left_known_to,
        right_known_from,
        right_known_to,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_rfc3339_shape_and_calendar_date() {
        for valid in [
            "2024-02-29T23:59:59Z",
            "2026-09-27t10:11:12.123z",
            "2026-09-27T10:11:60+05:30",
        ] {
            assert!(is_valid_rfc3339(valid), "expected valid: {valid}");
        }

        for invalid in [
            "",
            "2023-02-29T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-01-01 00:00:00Z",
            "2026-01-01T24:00:00Z",
            "2026-01-01T00:00:00",
            "2026-01-01T00:00:00.Z",
        ] {
            assert!(!is_valid_rfc3339(invalid), "expected invalid: {invalid}");
        }
    }

    #[test]
    fn intervals_are_half_open_and_allow_unbounded_ends() {
        assert!(interval_contains("2026-01", Some("2026-03"), "2026-01"));
        assert!(!interval_contains("2026-01", Some("2026-03"), "2026-03"));
        assert!(interval_contains("2026-01", None, "9999-01"));
    }

    #[test]
    fn touching_intervals_do_not_overlap() {
        assert!(!intervals_overlap(
            "2026-01",
            Some("2026-02"),
            "2026-02",
            Some("2026-03")
        ));
        assert!(intervals_overlap(
            "2026-01",
            Some("2026-03"),
            "2026-02",
            None
        ));
        assert!(!intervals_overlap(
            "2026-02",
            Some("2026-02"),
            "2026-01",
            Some("2026-03")
        ));
    }
}
