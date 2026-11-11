// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Node-related types for the CockroachDB Admin API.

pub use cockroach_admin_types_versions::latest::node::*;

/// CockroachDB Node ID (internal representation)
///
/// This field is stored internally as a String to avoid questions
/// about size, signedness, etc - it can be treated as an arbitrary
/// unique identifier.
///
/// Note: This is an internal type used for CLI parsing and internal
/// storage, distinct from the published API `NodeId` type in the
/// versions crate which has a different structure.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize,
)]
pub struct InternalNodeId(pub String);

impl InternalNodeId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InternalNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for InternalNodeId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

// When parsing the underlying InternalNodeId, we force it to be interpreted
// as a String. Without this custom Deserialize implementation, we
// encounter parsing errors when querying endpoints which return the
// node ID as an integer.
impl<'de> serde::Deserialize<'de> for InternalNodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct InternalNodeIdVisitor;

        impl<'de> Visitor<'de> for InternalNodeIdVisitor {
            type Value = InternalNodeId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter
                    .write_str("a string or integer representing a node ID")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(InternalNodeId(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(InternalNodeId(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(InternalNodeId(value.to_string()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(InternalNodeId(value.to_string()))
            }
        }

        deserializer.deserialize_any(InternalNodeIdVisitor)
    }
}
