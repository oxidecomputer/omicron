// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use std::fmt;

impl_enum_type!(
    AlertClassEnum:

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
    pub enum AlertClass;

    Probe => b"probe"
    TestFoo => b"test.foo"
    TestFooBar => b"test.foo.bar"
    TestFooBaz => b"test.foo.baz"
    TestQuuxBar => b"test.quux.bar"
    TestQuuxBarBaz => b"test.quux.bar.baz"
);

impl AlertClass {
    pub fn as_str(&self) -> &'static str {
        nexus_types::alert::AlertClass::from(*self).as_str()
    }

    /// Returns `true` if this event class is only used for testing and should
    /// not be incldued in the public event class list API endpoint.
    pub fn is_test(&self) -> bool {
        nexus_types::alert::AlertClass::from(*self).is_test()
    }

    /// Returns a human-readable description string describing this event class.
    pub fn description(&self) -> &'static str {
        nexus_types::alert::AlertClass::from(*self).description()
    }

    /// All webhook event classes.
    pub const ALL_CLASSES: &'static [Self] =
        <Self as strum::VariantArray>::VARIANTS;
}

impl From<nexus_types::alert::AlertClass> for AlertClass {
    fn from(input: nexus_types::alert::AlertClass) -> Self {
        use nexus_types::alert::AlertClass as In;
        match input {
            In::Probe => Self::Probe,
            In::TestFoo => Self::TestFoo,
            In::TestFooBar => Self::TestFooBar,
            In::TestFooBaz => Self::TestFooBaz,
            In::TestQuuxBar => Self::TestQuuxBar,
            In::TestQuuxBarBaz => Self::TestQuuxBarBaz,
        }
    }
}

impl From<AlertClass> for nexus_types::alert::AlertClass {
    fn from(input: AlertClass) -> Self {
        match input {
            AlertClass::Probe => Self::Probe,
            AlertClass::TestFoo => Self::TestFoo,
            AlertClass::TestFooBar => Self::TestFooBar,
            AlertClass::TestFooBaz => Self::TestFooBaz,
            AlertClass::TestQuuxBar => Self::TestQuuxBar,
            AlertClass::TestQuuxBarBaz => Self::TestQuuxBarBaz,
        }
    }
}

impl fmt::Display for AlertClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Serialize for AlertClass {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        nexus_types::alert::AlertClass::from(*self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AlertClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        nexus_types::alert::AlertClass::deserialize(deserializer)
            .map(Self::from)
    }
}
