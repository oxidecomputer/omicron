// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone)]
    #[diesel(postgres_type(name = "webhook_event_class", schema = "public"))]
    pub struct WebhookEventClassEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        strum::VariantArray,
    )]
    #[diesel(sql_type = WebhookEventClassEnum)]
    pub enum WebhookEventClass;

    TestFoo => b"test.foo"
    TestFooBar => b"test.foo.bar"
    TestBazBar => b"test.baz.bar"
);

impl WebhookEventClass {
    pub fn as_str(&self) -> &'static str {
        // TODO(eliza): it would be really nice if these strings were all
        // declared a single time, rather than twice (in both `impl_enum_type!`
        // and here)...
        match self {
            Self::TestFoo => "test.foo",
            Self::TestFooBar => "test.foo.bar",
            Self::TestBazBar => "test.baz.bar",
        }
    }

    /// All webhook event classes.
    pub const ALL_CLASSES: &'static [Self] =
        <Self as strum::VariantArray>::VARIANTS;
}

impl fmt::Display for WebhookEventClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl diesel::query_builder::QueryId for WebhookEventClassEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
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
}
