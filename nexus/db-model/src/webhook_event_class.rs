// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::de::{self, Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use std::fmt;

impl_enum_type!(
    WebhookEventClassEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        Hash,
        AsExpression,
        FromSqlRow,
        strum::VariantArray,
    )]
    pub enum WebhookEventClass;

    Probe => b"probe"
    TestFoo => b"test.foo"
    TestFooBar => b"test.foo.bar"
    TestFooBaz => b"test.foo.baz"
    TestQuuxBar => b"test.quux.bar"
    TestQuuxBarBaz => b"test.quux.bar.baz"
);

impl WebhookEventClass {
    pub fn as_str(&self) -> &'static str {
        // TODO(eliza): it would be really nice if these strings were all
        // declared a single time, rather than twice (in both `impl_enum_type!`
        // and here)...
        match self {
            Self::Probe => "probe",
            Self::TestFoo => "test.foo",
            Self::TestFooBar => "test.foo.bar",
            Self::TestFooBaz => "test.foo.baz",
            Self::TestQuuxBar => "test.quux.bar",
            Self::TestQuuxBarBaz => "test.quux.bar.baz",
        }
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

    /// All webhook event classes.
    pub const ALL_CLASSES: &'static [Self] =
        <Self as strum::VariantArray>::VARIANTS;
}

// Alphabetical ordering
impl Ord for WebhookEventClass {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for WebhookEventClass {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for WebhookEventClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Serialize for WebhookEventClass {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for WebhookEventClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <&'de str>::deserialize(deserializer)?
            .parse::<WebhookEventClass>()
            .map_err(de::Error::custom)
    }
}

impl std::str::FromStr for WebhookEventClass {
    type Err = EventClassParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for &class in Self::ALL_CLASSES {
            if s == class.as_str() {
                return Ok(class);
            }
        }

        Err(EventClassParseError(()))
    }
}

impl From<WebhookEventClass> for views::EventClass {
    fn from(class: WebhookEventClass) -> Self {
        Self {
            name: class.to_string(),
            description: class.description().to_string(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct EventClassParseError(());

impl fmt::Display for EventClassParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "expected one of [")?;
        let mut variants = WebhookEventClass::ALL_CLASSES.iter();
        if let Some(v) = variants.next() {
            write!(f, "{v}")?;
            for v in variants {
                write!(f, ", {v}")?;
            }
        }
        f.write_str("]")
    }
}

impl std::error::Error for EventClassParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_roundtrips() {
        for &variant in WebhookEventClass::ALL_CLASSES {
            assert_eq!(Ok(dbg!(variant)), dbg!(variant.to_string().parse()));
        }
    }

    // This is mainly a regression test to ensure that, should anyone add new
    // `test.` variants in future, the `WebhookEventClass::is_test()` method
    // returns `true` for them.
    #[test]
    fn test_is_test() {
        let problematic_variants = WebhookEventClass::ALL_CLASSES
            .iter()
            .copied()
            .filter(|variant| {
                variant.as_str().starts_with("test.") && !variant.is_test()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            problematic_variants,
            Vec::<WebhookEventClass>::new(),
            "you have added one or more new `test.*` webhook event class \
             variant(s), but you seem to have not updated the \
             `WebhookEventClass::is_test()` method!\nthe problematic \
             variant(s) are: {problematic_variants:?}",
        );
    }
}
