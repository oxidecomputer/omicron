// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal alert types.

use std::fmt;

/// Alert classes.
///
/// This is an internal, structured representation of the list of all alert
/// classes as an enum.
///
/// Note that this type is distinct from the
/// [`shared::AlertSubscription`](crate::external_api::shared::AlertSubscription)
/// type, which represents an alert *subscription* rather than a single alert
/// class. A subscription may be to a single alert class, *or* to a glob pattern
/// that matches multiple alert classes. The
/// [`external_api::views::AlertClass`](crate::external_api::views::AlertClass)
/// type represents the response message to the alert class view API and
/// contains a human-readable description of that alert class.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    strum::EnumString,
    strum::Display,
    strum::IntoStaticStr,
    strum::VariantArray,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(
    parse_err_fn = AlertClassParseError::from_input,
    parse_err_ty = AlertClassParseError,
)]
pub enum AlertClass {
    // TODO(eliza): it would be really nice if these strings were all
    // declared a single time, rather than twice (in both `impl_enum_type!`
    // macro in the db model and here)...
    #[strum(serialize = "probe")]
    Probe,
    #[strum(serialize = "test.foo")]
    TestFoo,
    #[strum(serialize = "test.foo.bar")]
    TestFooBar,
    #[strum(serialize = "test.foo.baz")]
    TestFooBaz,
    #[strum(serialize = "test.quux.bar")]
    TestQuuxBar,
    #[strum(serialize = "test.quux.bar.baz")]
    TestQuuxBarBaz,
}

impl AlertClass {
    pub fn as_str(&self) -> &'static str {
        // This is just a wrapper for the `strum::IntoStaticStr` derive.
        self.into()
    }

    /// Returns `true` if this event class is only used for testing and should
    /// not be incldued in the public event class list API endpoint.
    pub fn is_test(&self) -> bool {
        matches!(
            self,
            Self::TestFoo
                | Self::TestFooBar
                | Self::TestFooBaz
                | Self::TestQuuxBar
                | Self::TestQuuxBarBaz
        )
    }

    /// Returns a human-readable description string describing this event class.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Probe => {
                "Synthetic events sent for webhook receiver liveness probes.\n\
                 Receivers should return 2xx HTTP responses for these events, \
                 but they should NOT be treated as notifications of an actual \
                 event in the system."
            }
            Self::TestFoo
            | Self::TestFooBar
            | Self::TestFooBaz
            | Self::TestQuuxBar
            | Self::TestQuuxBarBaz => {
                "This is a test of the emergency alert system"
            }
        }
    }

    pub const ALL_CLASSES: &[Self] = <Self as strum::VariantArray>::VARIANTS;
}

impl From<AlertClass> for crate::external_api::views::AlertClass {
    fn from(class: AlertClass) -> Self {
        Self {
            name: class.to_string(),
            description: class.description().to_string(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct AlertClassParseError(());

impl fmt::Display for AlertClassParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "expected one of [")?;
        let mut variants = AlertClass::ALL_CLASSES.iter();
        if let Some(v) = variants.next() {
            write!(f, "{v}")?;
            for v in variants {
                write!(f, ", {v}")?;
            }
        }
        f.write_str("]")
    }
}

impl AlertClassParseError {
    // Strum's derive requires that this function take a &str, but we ignore
    // it.
    fn from_input(_: &str) -> Self {
        Self(())
    }
}

impl std::error::Error for AlertClassParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_roundtrips() {
        for &variant in AlertClass::ALL_CLASSES {
            assert_eq!(Ok(dbg!(variant)), dbg!(variant.to_string().parse()));
        }
    }

    // This is mainly a regression test to ensure that, should anyone add new
    // `test.` variants in future, the `AlertClass::is_test()` method
    // returns `true` for them.
    #[test]
    fn test_is_test() {
        let problematic_variants = AlertClass::ALL_CLASSES
            .iter()
            .copied()
            .filter(|variant| {
                variant.as_str().starts_with("test.") && !variant.is_test()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            problematic_variants,
            Vec::<AlertClass>::new(),
            "you have added one or more new `test.*` alert class \
             variant(s), but you seem to have not updated the \
             `AlertClass::is_test()` method!\nthe problematic \
             variant(s) are: {problematic_variants:?}",
        );
    }
}
