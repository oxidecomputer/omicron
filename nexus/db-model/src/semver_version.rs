// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use parse_display::Display;
use serde::{Deserialize, Serialize};

// We wrap semver::Version in external to impl JsonSchema, and we wrap it again
// here to impl ToSql/FromSql

/// Semver version with zero-padded numbers in `ToSql`/`FromSql` to allow
/// lexicographic DB sorting
#[derive(
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    Display,
)]
#[diesel(sql_type = sql_types::Text)]
#[display("{0}")]
pub struct SemverVersion(pub external::SemverVersion);

NewtypeFrom! { () pub struct SemverVersion(external::SemverVersion); }
NewtypeDeref! { () pub struct SemverVersion(external::SemverVersion); }

/// Width of version numbers after zero-padding. `u8` because you can always
/// convert to both `u32` and `usize`. Everything having to do with ser/de on
/// the version strings below is derived from this value, so in order to
/// increase or decrease the padding amount, you only have to change this (and
/// the tests).
const PADDED_WIDTH: u8 = 8;

impl SemverVersion {
    pub fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(external::SemverVersion(semver::Version::new(major, minor, patch)))
    }
}

/// Pad the version numbers with zeros so the result is lexicographically
/// sortable, e.g., `0.1.2` becomes `00000000.00000001.00000002`.
///
/// This requires that we impose a maximum size on each of the numbers so as not
/// to exceed the available number of digits.
///
/// An important caveat is that while lexicographic sort with padding does work
/// for the numeric part of the version string, it does not technically satisfy
/// the semver spec's rules for sorting pre-release and build metadata. Build
/// metadata is supposed to be ignored. Pre-release has more complicated rules,
/// most notably that a version *with* a pre-release string on it has lower
/// precedence than one *without*. See: <https://semver.org/#spec-item-11>. We
/// have decided sorting these wrong is tolerable for now. We can revisit later
/// if necessary.
///
/// Compare to the `Display` implementation on `Semver::Version`
/// <https://github.com/dtolnay/semver/blob/7fd09f7/src/display.rs>
fn to_sortable_string(v: semver::Version) -> Result<String, external::Error> {
    // the largest N-digit number is 10^N - 1
    let max = u64::pow(10, u32::from(PADDED_WIDTH)) - 1;

    if v.major > max || v.minor > max || v.patch > max {
        return Err(external::Error::invalid_value(
            "version",
            format!("Major, minor, and patch version must be less than {max}"),
        ));
    }

    let mut result = format!(
        "{:0>0width$}.{:0>0width$}.{:0>0width$}",
        v.major,
        v.minor,
        v.patch,
        width = usize::from(PADDED_WIDTH)
    );

    if !v.pre.is_empty() {
        result.push_str(&format!("-{}", v.pre));
    }
    if !v.build.is_empty() {
        result.push_str(&format!("+{}", v.build));
    }

    Ok(result)
}

/// Take the zero padding back out of the semver string. Assume that it's a
/// valid semver string aside from the padding, because we wrote it ourselves.
/// If it's not, the resulting string will fail `semver::Version`'s rather
/// strict parser.
fn from_sortable_string(s: String) -> String {
    // if there's no - or +, we use length as the split idx so rest = ""
    let delim_idx = s.find(&['-', '+']).unwrap_or_else(|| s.len());
    let (nums, rest) = s.split_at(delim_idx);

    // we split `rest` off first because otherwise the "" => "0" trick below
    // would not work

    let nums = nums
        .split('.')
        // Turn `"0010"` into `"10"` and `"0000"` into `"0"`
        .map(|s| match s.trim_start_matches('0') {
            // assuming, as we are, that the string is non-empty, the only way
            // we end up with an empty string is if it was all zeros
            "" => "0",
            x => x,
        })
        .collect::<Vec<_>>()
        .join(".");

    format!("{}{}", nums, rest)
}

impl<DB> ToSql<sql_types::Text, DB> for SemverVersion
where
    for<'c> DB: Backend<BindCollector<'c> = RawBytesBindCollector<DB>>,
    String: ToSql<sql_types::Text, DB>,
{
    fn to_sql<'b>(
        &'b self,
        out: &mut serialize::Output<'b, '_, DB>,
    ) -> serialize::Result {
        let v = (self.0).0.to_owned();
        to_sortable_string(v)?.to_sql(&mut out.reborrow())
    }
}

impl<DB> FromSql<sql_types::Text, DB> for SemverVersion
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    /// Remove the zero-padding before running it through
    /// `semver::Version::parse` because the extra zeros are not allowed.
    fn from_sql(raw: DB::RawValue<'_>) -> deserialize::Result<Self> {
        from_sortable_string(String::from_sql(raw)?)
            .parse()
            .map(|s| SemverVersion(external::SemverVersion(s)))
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use semver;

    #[test]
    fn test_to_from_sortable_string() {
        let pairs = [
            ("1.0.2", "00000001.00000000.00000002"),
            ("1.0.2-0.3.7", "00000001.00000000.00000002-0.3.7"),
            ("0.0.0-abc", "00000000.00000000.00000000-abc"),
            ("1.0.2-abc", "00000001.00000000.00000002-abc"),
            ("1.0.2+abc", "00000001.00000000.00000002+abc"),
            // don't strip zeros after a period in build meta
            ("1.2.0+a.000d--32", "00000001.00000002.00000000+a.000d--32"),
            ("1.0.2-abc+def", "00000001.00000000.00000002-abc+def"),
        ];
        for (orig, padded) in pairs {
            let v = orig.parse::<semver::Version>().unwrap();
            assert_eq!(&to_sortable_string(v).unwrap(), padded);
            assert_eq!(&from_sortable_string(padded.to_string()), orig);
        }
    }
}
