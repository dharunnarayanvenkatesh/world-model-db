//! Dependency-free validation and predicates for canonical RFC 3339 timestamps.
//!
//! Interval functions use half-open bounds (`from <= at < to`). Full RFC 3339
//! instants are compared after conversion to UTC, so equivalent timestamps with
//! different offsets have the same ordering.

use std::cmp::Ordering;
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

/// Converts a validated RFC 3339 instant to a fixed-width UTC representation.
/// Fractional seconds are retained and trailing zeroes are removed.
pub fn canonicalize_rfc3339(value: &str) -> Result<String, InvalidTimestamp> {
    validate_rfc3339(value)?;
    let bytes = value.as_bytes();
    let year = decimal(bytes, 0, 4).unwrap() as i64;
    let month = decimal(bytes, 5, 7).unwrap() as i64;
    let day = decimal(bytes, 8, 10).unwrap() as i64;
    let hour = decimal(bytes, 11, 13).unwrap() as i64;
    let minute = decimal(bytes, 14, 16).unwrap() as i64;
    let second = decimal(bytes, 17, 19).unwrap() as i64;
    let timezone = value
        .char_indices()
        .skip(19)
        .find(|(_, character)| matches!(character, 'Z' | 'z' | '+' | '-'))
        .map(|(index, _)| index)
        .unwrap();
    let fraction = if bytes.get(19) == Some(&b'.') {
        value[20..timezone].trim_end_matches('0')
    } else {
        ""
    };
    let offset_seconds = match bytes[timezone] {
        b'+' => {
            let h = decimal(bytes, timezone + 1, timezone + 3).unwrap() as i64;
            let m = decimal(bytes, timezone + 4, timezone + 6).unwrap() as i64;
            h * 3600 + m * 60
        }
        b'-' => {
            let h = decimal(bytes, timezone + 1, timezone + 3).unwrap() as i64;
            let m = decimal(bytes, timezone + 4, timezone + 6).unwrap() as i64;
            -(h * 3600 + m * 60)
        }
        _ => 0,
    };
    let epoch = days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second
        - offset_seconds;
    let days = epoch.div_euclid(86_400);
    let time = epoch.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    if !(0..=9999).contains(&year) {
        return Err(InvalidTimestamp {
            value: value.to_owned(),
        });
    }
    let suffix = if fraction.is_empty() {
        String::new()
    } else {
        format!(".{fraction}")
    };
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}{suffix}Z",
        time / 3600,
        time % 3600 / 60,
        time % 60
    ))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

/// Compares two RFC 3339 instants by UTC time, including arbitrary-precision
/// fractional seconds. Returns `None` when either input is invalid.
pub fn compare_rfc3339(left: &str, right: &str) -> Option<Ordering> {
    let (left_seconds, left_fraction) = instant_parts(left)?;
    let (right_seconds, right_fraction) = instant_parts(right)?;
    Some(
        left_seconds
            .cmp(&right_seconds)
            .then_with(|| compare_fraction(left_fraction, right_fraction)),
    )
}

fn instant_parts(value: &str) -> Option<(i64, &str)> {
    is_valid_rfc3339(value).then_some(())?;
    let bytes = value.as_bytes();
    let timezone = value
        .char_indices()
        .skip(19)
        .find(|(_, character)| matches!(character, 'Z' | 'z' | '+' | '-'))?
        .0;
    let year = decimal(bytes, 0, 4)? as i64;
    let month = decimal(bytes, 5, 7)? as i64;
    let day = decimal(bytes, 8, 10)? as i64;
    let hour = decimal(bytes, 11, 13)? as i64;
    let minute = decimal(bytes, 14, 16)? as i64;
    let second = decimal(bytes, 17, 19)? as i64;
    let offset = match bytes[timezone] {
        b'+' => {
            decimal(bytes, timezone + 1, timezone + 3)? as i64 * 3600
                + decimal(bytes, timezone + 4, timezone + 6)? as i64 * 60
        }
        b'-' => {
            -(decimal(bytes, timezone + 1, timezone + 3)? as i64 * 3600
                + decimal(bytes, timezone + 4, timezone + 6)? as i64 * 60)
        }
        _ => 0,
    };
    let fraction = if bytes.get(19) == Some(&b'.') {
        &value[20..timezone]
    } else {
        ""
    };
    Some((
        days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second - offset,
        fraction,
    ))
}

fn compare_fraction(left: &str, right: &str) -> Ordering {
    let width = left.len().max(right.len());
    left.bytes()
        .chain(std::iter::repeat(b'0'))
        .zip(right.bytes().chain(std::iter::repeat(b'0')))
        .take(width)
        .find_map(|(left, right)| (left != right).then(|| left.cmp(&right)))
        .unwrap_or(Ordering::Equal)
}

fn compare_or_lexical(left: &str, right: &str) -> Ordering {
    compare_rfc3339(left, right).unwrap_or_else(|| left.cmp(right))
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
    compare_or_lexical(from, at) != Ordering::Greater
        && to.is_none_or(|exclusive_end| compare_or_lexical(at, exclusive_end) == Ordering::Less)
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
    let left_is_nonempty =
        left_to.is_none_or(|left_end| compare_or_lexical(left_from, left_end) == Ordering::Less);
    let right_is_nonempty = right_to
        .is_none_or(|right_end| compare_or_lexical(right_from, right_end) == Ordering::Less);

    left_is_nonempty
        && right_is_nonempty
        && left_to.is_none_or(|left_end| compare_or_lexical(right_from, left_end) == Ordering::Less)
        && right_to
            .is_none_or(|right_end| compare_or_lexical(left_from, right_end) == Ordering::Less)
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
    fn canonicalizes_offsets_and_rolls_calendar_boundaries() {
        assert_eq!(
            canonicalize_rfc3339("2026-01-01T10:00:00+05:30").unwrap(),
            "2026-01-01T04:30:00Z"
        );
        assert_eq!(
            canonicalize_rfc3339("2025-12-31T23:30:00.1200-01:00").unwrap(),
            "2026-01-01T00:30:00.12Z"
        );
        assert!(interval_contains(
            "2026-01-01T04:00:00Z",
            Some("2026-01-01T05:00:00Z"),
            "2026-01-01T10:29:00+05:30"
        ));
        assert_eq!(
            compare_rfc3339("2026-01-01T00:00:00Z", "2026-01-01T00:00:00.1Z"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_rfc3339("2026-01-01T00:00:00.10Z", "2026-01-01T00:00:00.1Z"),
            Some(Ordering::Equal)
        );
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
