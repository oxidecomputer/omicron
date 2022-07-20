// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that are used as both views and params

use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::de::Error as _;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Maximum number of role assignments allowed on any one resource
// Today's implementation assumes a relatively small number of role assignments
// per resource.  Things should work if we bump this up, but we'll want to look
// into scalability improvements (e.g., use pagination for fetching and updating
// the role assignments, and consider the impact on authz checks as well).
//
// Most importantly: by keeping this low to start with, it's impossible for
// customers to develop a dependency on a huge number of role assignments.  That
// maximizes our flexibility in the future.
//
// TODO This should be runtime-configurable.  But it doesn't belong in the Nexus
// configuration file, since it's a constraint on database objects more than it
// is Nexus.  We should have some kinds of config that lives in the database.
pub const MAX_ROLE_ASSIGNMENTS_PER_RESOURCE: usize = 64;

/// Client view of a [`Policy`], which describes how this resource may be
/// accessed
///
/// Note that the Policy only describes access granted explicitly for this
/// resource.  The policies of parent resources can also cause a user to have
/// access to this resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}Policy")]
pub struct Policy<AllowedRoles: serde::de::DeserializeOwned> {
    /// Roles directly assigned on this resource
    #[serde(deserialize_with = "role_assignments_deserialize")]
    pub role_assignments: Vec<RoleAssignment<AllowedRoles>>,
}

fn role_assignments_deserialize<'de, D, R>(
    d: D,
) -> Result<Vec<RoleAssignment<R>>, D::Error>
where
    D: Deserializer<'de>,
    R: serde::de::DeserializeOwned,
{
    let v = Vec::<_>::deserialize(d)?;
    if v.len() > MAX_ROLE_ASSIGNMENTS_PER_RESOURCE {
        return Err(D::Error::invalid_length(
            v.len(),
            &format!(
                "a list of at most {} role assignments",
                MAX_ROLE_ASSIGNMENTS_PER_RESOURCE
            )
            .as_str(),
        ));
    }
    Ok(v)
}

/// Describes the assignment of a particular role on a particular resource to a
/// particular identity (user, group, etc.)
///
/// The resource is not part of this structure.  Rather, [`RoleAssignment`]s are
/// put into a [`Policy`] and that Policy is applied to a particular resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}RoleAssignment")]
pub struct RoleAssignment<AllowedRoles> {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub role_name: AllowedRoles,
}

/// Describes what kind of identity is described by an id
// This is a subset of the identity types that might be found in the database
// because we do not expose some (e.g., built-in users) externally.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IdentityType {
    SiloUser,
}

/// How users will be provisioned in a silo during authentication.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UserProvisionType {
    /// Do not automatically create users during authentication if they do not
    /// exist in the database already.
    Fixed,

    /// Create users during authentication if they do not exist in the database
    /// already, using information provided by the identity provider.
    Jit,
}

/// An IP Range is a contiguous range of IP addresses, usually within an IP
/// Pool.
///
/// The first address in the range is guaranteed to be no greater than the last
/// address.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum IpRange {
    V4(Ipv4Range),
    V6(Ipv6Range),
}

// NOTE: We don't derive JsonSchema. That's intended so that we can use an
// untagged enum for `IpRange`, and use this method to annotate schemars output
// for client-generators (e.g., progenitor) to use in generating a better
// client.
impl JsonSchema for IpRange {
    fn schema_name() -> String {
        "IpRange".to_string()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
                one_of: Some(vec![
                    omicron_common::api::external::label_schema(
                        "v4",
                        gen.subschema_for::<Ipv4Range>(),
                    ),
                    omicron_common::api::external::label_schema(
                        "v6",
                        gen.subschema_for::<Ipv6Range>(),
                    ),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

impl IpRange {
    pub fn first_address(&self) -> IpAddr {
        match self {
            IpRange::V4(inner) => IpAddr::from(inner.first),
            IpRange::V6(inner) => IpAddr::from(inner.first),
        }
    }

    pub fn last_address(&self) -> IpAddr {
        match self {
            IpRange::V4(inner) => IpAddr::from(inner.last),
            IpRange::V6(inner) => IpAddr::from(inner.last),
        }
    }
}

impl TryFrom<(Ipv4Addr, Ipv4Addr)> for IpRange {
    type Error = String;

    fn try_from(pair: (Ipv4Addr, Ipv4Addr)) -> Result<Self, Self::Error> {
        Ipv4Range::new(pair.0, pair.1).map(IpRange::V4)
    }
}

impl TryFrom<(Ipv6Addr, Ipv6Addr)> for IpRange {
    type Error = String;

    fn try_from(pair: (Ipv6Addr, Ipv6Addr)) -> Result<Self, Self::Error> {
        Ipv6Range::new(pair.0, pair.1).map(IpRange::V6)
    }
}

/// A non-decreasing IPv4 address range, inclusive of both ends.
///
/// The first address must be less than or equal to the last address.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(try_from = "AnyIpv4Range")]
pub struct Ipv4Range {
    first: Ipv4Addr,
    last: Ipv4Addr,
}

impl Ipv4Range {
    pub fn new(first: Ipv4Addr, last: Ipv4Addr) -> Result<Self, String> {
        if first <= last {
            Ok(Self { first, last })
        } else {
            Err(String::from("IP address ranges must be non-decreasing"))
        }
    }

    pub fn first_address(&self) -> Ipv4Addr {
        self.first
    }

    pub fn last_address(&self) -> Ipv4Addr {
        self.last
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct AnyIpv4Range {
    first: Ipv4Addr,
    last: Ipv4Addr,
}

impl TryFrom<AnyIpv4Range> for Ipv4Range {
    type Error = Error;
    fn try_from(r: AnyIpv4Range) -> Result<Self, Self::Error> {
        Ipv4Range::new(r.first, r.last)
            .map_err(|msg| Error::invalid_request(msg.as_str()))
    }
}

/// A non-decreasing IPv6 address range, inclusive of both ends.
///
/// The first address must be less than or equal to the last address.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(try_from = "AnyIpv6Range")]
pub struct Ipv6Range {
    first: Ipv6Addr,
    last: Ipv6Addr,
}

impl Ipv6Range {
    pub fn new(first: Ipv6Addr, last: Ipv6Addr) -> Result<Self, String> {
        if first <= last {
            Ok(Self { first, last })
        } else {
            Err(String::from("IP address ranges must be non-decreasing"))
        }
    }

    pub fn first_address(&self) -> Ipv6Addr {
        self.first
    }

    pub fn last_address(&self) -> Ipv6Addr {
        self.last
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct AnyIpv6Range {
    first: Ipv6Addr,
    last: Ipv6Addr,
}

impl TryFrom<AnyIpv6Range> for Ipv6Range {
    type Error = Error;
    fn try_from(r: AnyIpv6Range) -> Result<Self, Self::Error> {
        Ipv6Range::new(r.first, r.last)
            .map_err(|msg| Error::invalid_request(msg.as_str()))
    }
}

/// The kind of an external IP address for an instance
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IpKind {
    Ephemeral,
    Floating,
}

#[cfg(test)]
mod test {
    use super::IdentityType;
    use super::Policy;
    use super::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
    use crate::external_api::shared;
    use crate::external_api::shared::IpRange;
    use crate::external_api::shared::Ipv4Range;
    use crate::external_api::shared::Ipv6Range;
    use anyhow::anyhow;
    use omicron_common::api::external::Error;
    use serde::Deserialize;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    pub enum DummyRoles {
        Bogus,
    }

    #[test]
    fn test_policy_parsing() {
        // Success case (edge case: max number of role assignments)
        let role_assignment = serde_json::json!({
            "identity_type": "silo_user",
            "identity_id": "75ec4a39-67cf-4549-9e74-44b92947c37c",
            "role_name": "bogus"
        });
        const MAX: usize = MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
        let okay_input =
            serde_json::Value::Array(vec![role_assignment.clone(); MAX]);
        let policy: Policy<DummyRoles> =
            serde_json::from_value(serde_json::json!({
                "role_assignments": okay_input
            }))
            .expect("unexpectedly failed with okay input");
        assert_eq!(policy.role_assignments[0].role_name, DummyRoles::Bogus);

        // Failure case: too many role assignments
        let bad_input =
            serde_json::Value::Array(vec![role_assignment; MAX + 1]);
        let error =
            serde_json::from_value::<Policy<DummyRoles>>(serde_json::json!({
                "role_assignments": bad_input
            }))
            .expect_err("unexpectedly succeeded with too many items");
        assert_eq!(
            error.to_string(),
            "invalid length 65, expected a list of at most 64 role assignments"
        );
    }

    #[test]
    fn test_ip_range_checks_non_decreasing() {
        let lo = Ipv4Addr::new(10, 0, 0, 1);
        let hi = Ipv4Addr::new(10, 0, 0, 3);
        assert!(Ipv4Range::new(lo, hi).is_ok());
        assert!(Ipv4Range::new(lo, lo).is_ok());
        assert!(Ipv4Range::new(hi, lo).is_err());

        let lo = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let hi = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3);
        assert!(Ipv6Range::new(lo, hi).is_ok());
        assert!(Ipv6Range::new(lo, lo).is_ok());
        assert!(Ipv6Range::new(hi, lo).is_err());
    }

    #[test]
    fn test_ip_range_enum_deserialization() {
        let data = r#"{"first": "10.0.0.1", "last": "10.0.0.3"}"#;
        let expected = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(10, 0, 0, 3),
            )
            .unwrap(),
        );
        assert_eq!(expected, serde_json::from_str(data).unwrap());

        let data = r#"{"first": "fd00::", "last": "fd00::3"}"#;
        let expected = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0),
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3),
            )
            .unwrap(),
        );
        assert_eq!(expected, serde_json::from_str(data).unwrap());

        let data = r#"{"first": "fd00::3", "last": "fd00::"}"#;
        assert!(
            serde_json::from_str::<IpRange>(data).is_err(),
            "Expected an error deserializing an IP range with first address \
            greater than last address",
        );
    }

    #[test]
    fn test_ip_range_try_from() {
        let lo = Ipv4Addr::new(10, 0, 0, 1);
        let hi = Ipv4Addr::new(10, 0, 0, 3);
        assert!(IpRange::try_from((lo, hi)).is_ok());
        assert!(IpRange::try_from((hi, lo)).is_err());

        let lo = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let hi = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3);
        assert!(IpRange::try_from((lo, hi)).is_ok());
        assert!(IpRange::try_from((hi, lo)).is_err());
    }
}
