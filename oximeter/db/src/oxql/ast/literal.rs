// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST node for literal values.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::cmp::Comparison;
use chrono::DateTime;
use chrono::Utc;
use oximeter::FieldType;
use oximeter::FieldValue;
use regex::Regex;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;
use uuid::Uuid;

/// A literal value.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    // TODO-performance: An i128 here is a bit gratuitous.
    Integer(i128),
    Double(f64),
    String(String),
    Boolean(bool),
    Uuid(Uuid),
    Duration(Duration),
    Timestamp(DateTime<Utc>),
    IpAddr(IpAddr),
}

impl Literal {
    // Format the literal as a safe, typed string for ClickHouse.
    pub(crate) fn as_db_safe_string(&self) -> String {
        match self {
            Literal::Integer(inner) => format!("{inner}"),
            Literal::Double(inner) => format!("{inner}"),
            Literal::String(inner) => format!("'{inner}'"),
            Literal::Boolean(inner) => format!("{inner}"),
            Literal::Uuid(inner) => format!("'{inner}'"),
            Literal::Duration(inner) => {
                let (count, interval) = duration_to_db_interval(inner);
                format!("INTERVAL {} {}", count, interval)
            }
            Literal::Timestamp(inner) => {
                format!("'{}'", inner.format(crate::DATABASE_TIMESTAMP_FORMAT))
            }
            Literal::IpAddr(inner) => {
                let fn_name = if inner.is_ipv6() { "toIPv6" } else { "toIPv4" };
                format!("{fn_name}('{inner}')")
            }
        }
    }

    // Return true if this literal can be compared to a field of the provided
    // type.
    pub(crate) fn is_compatible_with_field(
        &self,
        field_type: FieldType,
    ) -> bool {
        match self {
            Literal::Integer(_) => matches!(
                field_type,
                FieldType::U8
                    | FieldType::I8
                    | FieldType::U16
                    | FieldType::I16
                    | FieldType::U32
                    | FieldType::I32
                    | FieldType::U64
                    | FieldType::I64
            ),
            Literal::Double(_) => false,
            Literal::String(_) => matches!(field_type, FieldType::String),
            Literal::Boolean(_) => matches!(field_type, FieldType::Bool),
            Literal::Uuid(_) => matches!(field_type, FieldType::Uuid),
            Literal::Duration(_) => false,
            Literal::Timestamp(_) => false,
            Literal::IpAddr(_) => matches!(field_type, FieldType::IpAddr),
        }
    }

    // Apply the comparison op between self and the provided field.
    //
    // Return None if the comparison cannot be applied, either because the type
    // is not compatible or the comparison doesn't make sense.
    pub(crate) fn compare_field(
        &self,
        value: &FieldValue,
        cmp: Comparison,
    ) -> Option<bool> {
        if !self.is_compatible_with_field(value.field_type()) {
            return None;
        }
        macro_rules! generate_cmp_match {
            ($lhs:ident, $rhs:ident) => {
                match cmp {
                    Comparison::Eq => Some($lhs == $rhs),
                    Comparison::Ne => Some($lhs != $rhs),
                    Comparison::Gt => Some($lhs > $rhs),
                    Comparison::Ge => Some($lhs >= $rhs),
                    Comparison::Lt => Some($lhs < $rhs),
                    Comparison::Le => Some($lhs <= $rhs),
                    Comparison::Like => None,
                }
            };
        }
        // Filter expressions are currently written as `<ident> <cmp>
        // <literal>`. That means the literal stored in `self` is the RHS of
        // the comparison, and the field value passed in is the LHS.
        match (value, self) {
            (FieldValue::Bool(lhs), Literal::Boolean(rhs)) => {
                generate_cmp_match!(rhs, lhs)
            }
            (FieldValue::String(lhs), Literal::String(rhs)) => match cmp {
                Comparison::Eq => Some(lhs == rhs),
                Comparison::Ne => Some(lhs != rhs),
                Comparison::Gt => Some(lhs > rhs),
                Comparison::Ge => Some(lhs >= rhs),
                Comparison::Lt => Some(lhs < rhs),
                Comparison::Le => Some(lhs <= rhs),
                Comparison::Like => {
                    let re = Regex::new(rhs).ok()?;
                    Some(re.is_match(lhs))
                }
            },
            (FieldValue::IpAddr(lhs), Literal::IpAddr(rhs)) => {
                generate_cmp_match!(rhs, lhs)
            }
            (FieldValue::Uuid(lhs), Literal::Uuid(rhs)) => {
                generate_cmp_match!(rhs, lhs)
            }
            (FieldValue::U8(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::I8(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::U16(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::I16(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::U32(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::I32(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::U64(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (FieldValue::I64(lhs), Literal::Integer(rhs)) => {
                let lhs = i128::from(*lhs);
                let rhs = *rhs;
                generate_cmp_match!(lhs, rhs)
            }
            (_, _) => unreachable!(),
        }
    }
}

pub(crate) mod duration_consts {
    use std::time::Duration;
    pub const YEAR: Duration = Duration::from_secs(60 * 60 * 24 * 365);
    pub const MONTH: Duration = Duration::from_secs(60 * 60 * 24 * 30);
    pub const WEEK: Duration = Duration::from_secs(60 * 60 * 24 * 7);
    pub const DAY: Duration = Duration::from_secs(60 * 60 * 24);
    pub const HOUR: Duration = Duration::from_secs(60 * 60);
    pub const MINUTE: Duration = Duration::from_secs(60);
    pub const SECOND: Duration = Duration::from_secs(1);
    pub const MILLISECOND: Duration = Duration::from_millis(1);
    pub const MICROSECOND: Duration = Duration::from_micros(1);
    pub const NANOSECOND: Duration = Duration::from_nanos(1);
}

// Convert a duration into an appropriate interval for a database query.
//
// This converts the provided duration into the largest interval type for which
// the value is an integer. For example:
//
// `1us` -> (1, "MICROSECOND"),
// `3.4s` -> (3400, "MILLISECOND")
fn duration_to_db_interval(dur: &Duration) -> (u64, &'static str) {
    fn as_whole_multiple(dur: &Duration, base: &Duration) -> Option<u64> {
        let d = dur.as_nanos();
        let base = base.as_nanos();
        if d % base == 0 {
            Some(u64::try_from(d / base).unwrap())
        } else {
            None
        }
    }
    use duration_consts::*;
    const INTERVALS: [(Duration, &str); 10] = [
        (YEAR, "YEAR"),
        (MONTH, "MONTH"),
        (WEEK, "WEEK"),
        (DAY, "DAY"),
        (HOUR, "HOUR"),
        (MINUTE, "MINUTE"),
        (SECOND, "SECOND"),
        (MILLISECOND, "MILLISECOND"),
        (MICROSECOND, "MICROSECOND"),
        (NANOSECOND, "NANOSECOND"),
    ];
    for (base, interval) in &INTERVALS {
        if let Some(count) = as_whole_multiple(dur, base) {
            return (count, interval);
        }
    }

    // Durations must be a whole number of nanoseconds, so we will never fall
    // past the last interval in the array above.
    unreachable!();
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Literal::Integer(inner) => write!(f, "{inner}"),
            Literal::Double(inner) => write!(f, "{inner}"),
            Literal::String(inner) => write!(f, "{inner:?}"),
            Literal::Boolean(inner) => write!(f, "{inner}"),
            Literal::Uuid(inner) => write!(f, "\"{inner}\""),
            Literal::Duration(inner) => write!(f, "{inner:?}"),
            Literal::Timestamp(inner) => write!(f, "@{inner}"),
            Literal::IpAddr(inner) => write!(f, "{inner}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::duration_consts::*;
    use super::duration_to_db_interval;
    use super::Literal;
    use crate::oxql::ast::cmp::Comparison;
    use oximeter::FieldValue;

    #[test]
    fn test_duration_to_db_interval() {
        for base in [1_u32, 2, 3] {
            let b = u64::from(base);
            assert_eq!(duration_to_db_interval(&(base * YEAR)), (b, "YEAR"));
            assert_eq!(duration_to_db_interval(&(base * MONTH)), (b, "MONTH"));
            assert_eq!(duration_to_db_interval(&(base * WEEK)), (b, "WEEK"));
            assert_eq!(duration_to_db_interval(&(base * DAY)), (b, "DAY"));
            assert_eq!(duration_to_db_interval(&(base * HOUR)), (b, "HOUR"));
            assert_eq!(
                duration_to_db_interval(&(base * MINUTE)),
                (b, "MINUTE")
            );
            assert_eq!(
                duration_to_db_interval(&(base * SECOND)),
                (b, "SECOND")
            );
            assert_eq!(
                duration_to_db_interval(&(base * MILLISECOND)),
                (b, "MILLISECOND")
            );
            assert_eq!(
                duration_to_db_interval(&(base * MICROSECOND)),
                (b, "MICROSECOND")
            );
            assert_eq!(
                duration_to_db_interval(&(base * NANOSECOND)),
                (b, "NANOSECOND")
            );
        }
        assert_eq!(duration_to_db_interval(&(YEAR / 2)), (4380, "HOUR"));
        assert_eq!(duration_to_db_interval(&(HOUR / 60)), (1, "MINUTE"));
        assert_eq!(duration_to_db_interval(&(HOUR / 10)), (6, "MINUTE"));
        assert_eq!(duration_to_db_interval(&(HOUR / 12)), (5, "MINUTE"));
        assert_eq!(duration_to_db_interval(&(HOUR / 120)), (30, "SECOND"));
        assert_eq!(duration_to_db_interval(&(MINUTE / 2)), (30, "SECOND"));
        assert_eq!(duration_to_db_interval(&(MINUTE / 10)), (6, "SECOND"));
        assert_eq!(
            duration_to_db_interval(&MINUTE.mul_f64(1.5)),
            (90, "SECOND")
        );
        assert_eq!(
            duration_to_db_interval(&MICROSECOND.mul_f64(1.5)),
            (1500, "NANOSECOND")
        );
        assert_eq!(
            duration_to_db_interval(&(YEAR + NANOSECOND)),
            (31536000000000001, "NANOSECOND")
        );
    }

    #[test]
    fn test_literal_compare_field() {
        let value = FieldValue::I64(3);
        let lit = Literal::Integer(4);

        // The literal comparison would be written like: `field >= 4` where
        // `field` has a value of 3 here. So the comparison is false.
        assert_eq!(lit.compare_field(&value, Comparison::Ge).unwrap(), false);

        // Reversing this, we should have true.
        assert_eq!(lit.compare_field(&value, Comparison::Lt).unwrap(), true);

        // It should not be equal.
        assert_eq!(lit.compare_field(&value, Comparison::Eq).unwrap(), false);
        assert_eq!(lit.compare_field(&value, Comparison::Ne).unwrap(), true);
    }

    #[test]
    fn test_literal_compare_field_wrong_type() {
        let value = FieldValue::String(String::from("foo"));
        let lit = Literal::Integer(4);
        assert!(lit.compare_field(&value, Comparison::Eq).is_none());
    }
}
